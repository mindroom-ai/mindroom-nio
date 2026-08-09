"""Durable network-effect journal contract tests."""

import base64
import hashlib
import json
import math
import sqlite3
from dataclasses import fields, replace
from inspect import signature
from pathlib import Path
from uuid import UUID, uuid5

import pytest

from nio.ingest import (
    ConsumerBinding,
    ConsumerBootstrap,
    EventRecord,
    RecordKind,
    RecordOrigin,
    RoomHydrationStatus,
    TimelineEventProvenance,
    TransportKind,
)
from nio.ingest.config import ClassicSourceConfig, SlidingSourceConfig
from nio.ingest.effects import (
    MembershipAction,
    MembershipDeliveryState,
    MembershipRequest,
    PersistedNetworkEffect,
    RecoveryRequest,
    RoomHydrationRequest,
)
from nio.ingest.errors import JournalConflictError, JournalIntegrityError
from nio.ingest.membership import MembershipBaseline
from nio.ingest.recovery import RecoveryGap
from nio.ingest.serialization import _canonical_json, _record_to_dict
from nio.ingest.sliding import _sliding_cursor_from_json, canonical_sliding_cursor
from nio.ingest.state import (
    JournalTransition,
    LaneRecord,
    LaneRecordKey,
    LaneRecordSection,
    LaneStatus,
    ReleasePhase,
    RoomLane,
    RoomState,
    SourceState,
)
from nio.store._sync_journal_effects import NetworkEffectRows
from nio.store._sync_journal_port import IngestionJournal
from nio.store.sync_journal import open_ingestion_store

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "DEVICE"
JOURNAL_GENERATION = UUID("11111111-1111-1111-1111-111111111111")
CONSUMER_GENERATION = UUID("22222222-2222-2222-2222-222222222222")
MEMBERSHIP_EFFECT_ID = UUID("55555555-5555-5555-5555-555555555555")
GAP_ID = UUID("44444444-4444-4444-4444-444444444444")
CLASSIC_SOURCE = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")
_RAW_EFFECT_INSERT = """INSERT INTO NioIngestNetworkEffect (
    account_id, effect_id, effect_kind, room_id, membership_epoch,
    attempt_ordinal, membership_delivery_state, prior_delivery_uncertain,
    request_ciphertext, request_sha256, state_ciphertext, state_sha256,
    created_revision, updated_revision
) VALUES (
    :account_id, :effect_id, :effect_kind, :room_id, :membership_epoch,
    :attempt_ordinal, :membership_delivery_state, :prior_delivery_uncertain,
    :request_ciphertext, :request_sha256, :state_ciphertext, :state_sha256,
    :created_revision, :updated_revision
)"""


def _sliding_source() -> SlidingSourceConfig:
    return SlidingSourceConfig(
        30_000,
        "worker",
        b"{}",
        b"{}",
        b"{}",
        100,
    )


def _consumer(bootstrap, room_ids: tuple[str, ...]) -> ConsumerBootstrap:
    canonical = json.dumps(list(room_ids), separators=(",", ":")).encode()
    return ConsumerBootstrap(
        bootstrap.binding_operation_id,
        ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
        bootstrap.next_batch_sequence,
        room_ids,
        hashlib.sha256(canonical).digest(),
    )


def _membership(
    stream_id: UUID,
    *,
    transport: TransportKind = TransportKind.CLASSIC,
) -> PersistedNetworkEffect:
    return PersistedNetworkEffect(
        MembershipRequest(
            MEMBERSHIP_EFFECT_ID,
            stream_id,
            transport,
            "!membership",
            0,
            MembershipAction.JOIN,
            b'{"reason":"restore"}',
            30_000,
        ),
        0,
        MembershipDeliveryState.READY,
        False,
    )


def _hydration(
    stream_id: UUID,
    *,
    transport: TransportKind = TransportKind.CLASSIC,
) -> PersistedNetworkEffect:
    room_id = "!hydration"
    return PersistedNetworkEffect(
        RoomHydrationRequest(
            uuid5(stream_id, f"hydrate:{room_id}:0"),
            stream_id,
            transport,
            room_id,
            0,
            30_000,
        ),
        0,
        None,
        None,
    )


def _recovery(
    stream_id: UUID,
    *,
    room_id: str = "!recovery",
    gap_id: UUID = GAP_ID,
    transport: TransportKind = TransportKind.CLASSIC,
) -> tuple[PersistedNetworkEffect, RoomState, RoomLane]:
    effect_id = uuid5(gap_id, "page:1")
    effect = PersistedNetworkEffect(
        RecoveryRequest(
            effect_id,
            stream_id,
            transport,
            room_id,
            0,
            gap_id,
            1,
            "cursor-1",
            "target-token",
            100,
            30_000,
        ),
        0,
        None,
        None,
    )
    state = RoomState(
        room_id,
        0,
        0,
        RoomHydrationStatus.PENDING,
        None,
        MembershipBaseline(room_id, 4, 0, "start-token", "$member"),
    )
    gap = RecoveryGap(
        gap_id,
        room_id,
        4,
        0,
        RecordOrigin(transport, 4, 9, 2),
        "$member",
        "start-token",
        "target-token",
        "cursor-1",
        ("start-token", "cursor-1"),
        1,
        8,
        effect_id,
    )
    lane = RoomLane(
        room_id,
        0,
        LaneStatus.ACTIVE,
        release_phase=ReleasePhase.RECOVERING,
        recovery_gap=gap,
    )
    return effect, state, lane


def _effect_row_bytes(journal, effect_id: UUID) -> tuple[object, ...] | None:
    row = journal.connection.execute(
        "SELECT request_ciphertext, request_sha256, state_ciphertext, state_sha256, "
        "created_revision, updated_revision FROM NioIngestNetworkEffect "
        "WHERE account_id = ? AND effect_id = ?",
        (ACCOUNT_ID, str(effect_id)),
    ).fetchone()
    return tuple(row) if row is not None else None


def _canonical_membership_request_envelope(request: MembershipRequest) -> bytes:
    return _canonical_json(
        {
            "effect_id": str(request.effect_id),
            "effect_kind": "membership",
            "stream_id": str(request.stream_id),
            "transport": request.transport.value,
            "room_id": request.room_id,
            "membership_epoch": request.membership_epoch,
            "action": request.action.value,
            "request_body": base64.b64encode(request.request_body).decode("ascii"),
            "timeout_ms": request.timeout_ms,
        }
    )


def _effect_envelope(journal, effect_id: UUID, part: str) -> dict[str, object]:
    row = journal.connection.execute(
        "SELECT * FROM NioIngestNetworkEffect "
        "WHERE account_id = ? AND effect_id = ?",
        (ACCOUNT_ID, str(effect_id)),
    ).fetchone()
    payload = journal._open_payload(
        f"NioIngestNetworkEffect.{part}",
        (effect_id,),
        row,
        part,
    )
    value = json.loads(payload)
    assert type(value) is dict
    return value


def _reseal_effect_envelope(
    journal,
    effect_id: UUID,
    part: str,
    envelope: dict[str, object],
    *,
    payload: bytes | None = None,
) -> None:
    canonical = _canonical_json(envelope) if payload is None else payload
    ciphertext, digest = journal._codec.seal(
        f"NioIngestNetworkEffect.{part}",
        (effect_id,),
        canonical,
    )
    journal.connection.execute(
        f"UPDATE NioIngestNetworkEffect SET {part}_ciphertext = ?, "
        f"{part}_sha256 = ? WHERE account_id = ? AND effect_id = ?",
        (ciphertext, digest, ACCOUNT_ID, str(effect_id)),
    )


async def _journal_with_membership_effect(
    tmp_path: Path,
    *,
    statement_observer=None,
):
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
        statement_observer=statement_observer,
    )
    await bootstrap.attach_consumer(_consumer(bootstrap, ("!membership",)))
    effect = _membership(bootstrap.stream_id)
    inserted = bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=JournalTransition(network_effect_inserts=(effect,)),
    )
    return bootstrap, effect, inserted


def test_network_effect_schema_has_split_authenticated_envelopes_and_partial_owner(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
    )
    bootstrap.close()

    with sqlite3.connect(tmp_path / "journal.db") as connection:
        columns = tuple(
            row[1]
            for row in connection.execute('PRAGMA table_info("NioIngestNetworkEffect")')
        )
        table_sql = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type = 'table' "
            "AND name = 'NioIngestNetworkEffect'"
        ).fetchone()[0]
        indexes = tuple(
            connection.execute(
                "SELECT name, sql FROM sqlite_master WHERE type = 'index' "
                "AND tbl_name = 'NioIngestNetworkEffect' ORDER BY name"
            )
        )

    assert columns == (
        "account_id",
        "effect_id",
        "effect_kind",
        "room_id",
        "membership_epoch",
        "attempt_ordinal",
        "membership_delivery_state",
        "prior_delivery_uncertain",
        "request_ciphertext",
        "request_sha256",
        "state_ciphertext",
        "state_sha256",
        "created_revision",
        "updated_revision",
    )
    assert "source_epoch" not in columns
    assert "CHECK" in table_sql
    assert any(
        name == "NioIngestNetworkEffect_membership_room"
        and "WHERE effect_kind = 'membership'" in sql
        for name, sql in indexes
    )


def _raw_effect_row(**changes: object) -> dict[str, object]:
    row: dict[str, object] = {
        "account_id": ACCOUNT_ID,
        "effect_id": "11111111-1111-1111-1111-111111111111",
        "effect_kind": "recovery",
        "room_id": "!room",
        "membership_epoch": 0,
        "attempt_ordinal": 0,
        "membership_delivery_state": None,
        "prior_delivery_uncertain": None,
        "request_ciphertext": b"request",
        "request_sha256": b"r" * 32,
        "state_ciphertext": b"state",
        "state_sha256": b"s" * 32,
        "created_revision": 1,
        "updated_revision": 1,
    }
    row.update(changes)
    return row


def test_network_effect_schema_enforces_exact_union_state_and_indexes(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
    )
    bootstrap.close()

    database_path = tmp_path / "journal.db"
    with sqlite3.connect(database_path) as connection:
        valid_rows = (
            _raw_effect_row(),
            _raw_effect_row(
                effect_id="22222222-2222-2222-2222-222222222222",
                effect_kind="room_hydration",
                room_id="!hydration",
            ),
            _raw_effect_row(
                effect_id="33333333-3333-3333-3333-333333333333",
                effect_kind="membership",
                room_id="!membership-ready",
                membership_delivery_state="ready",
                prior_delivery_uncertain=0,
            ),
            _raw_effect_row(
                effect_id="44444444-4444-4444-4444-444444444444",
                effect_kind="membership",
                room_id="!membership-dispatched",
                attempt_ordinal=1,
                membership_delivery_state="dispatched_unconfirmed",
                prior_delivery_uncertain=0,
            ),
            _raw_effect_row(
                effect_id="55555555-5555-5555-5555-555555555555",
                effect_kind="membership",
                room_id="!membership-retried",
                attempt_ordinal=2,
                membership_delivery_state="dispatched_unconfirmed",
                prior_delivery_uncertain=1,
            ),
            _raw_effect_row(
                effect_id="66666666-6666-6666-6666-666666666666",
                effect_kind="membership",
                room_id="!membership-retry-ready",
                attempt_ordinal=1,
                membership_delivery_state="ready",
                prior_delivery_uncertain=1,
            ),
        )
        for row in valid_rows:
            connection.execute(_RAW_EFFECT_INSERT, row)

        invalid_changes = (
            {"effect_id": b"not-text"},
            {"effect_id": ""},
            {"effect_kind": "foreign"},
            {"room_id": b"not-text"},
            {"room_id": ""},
            {"membership_epoch": "invalid"},
            {"membership_epoch": -1},
            {"attempt_ordinal": "invalid"},
            {"attempt_ordinal": -1},
            {"membership_delivery_state": "ready"},
            {"prior_delivery_uncertain": 0},
            {
                "effect_kind": "membership",
                "membership_delivery_state": None,
                "prior_delivery_uncertain": 0,
            },
            {
                "effect_kind": "membership",
                "membership_delivery_state": "ready",
                "prior_delivery_uncertain": None,
            },
            {
                "effect_kind": "membership",
                "membership_delivery_state": "foreign",
                "prior_delivery_uncertain": 0,
            },
            {
                "effect_kind": "membership",
                "membership_delivery_state": "ready",
                "prior_delivery_uncertain": 2,
            },
            {"attempt_ordinal": 1},
            {
                "effect_kind": "membership",
                "attempt_ordinal": 0,
                "membership_delivery_state": "dispatched_unconfirmed",
                "prior_delivery_uncertain": 0,
            },
            {
                "effect_kind": "membership",
                "attempt_ordinal": 1,
                "membership_delivery_state": "dispatched_unconfirmed",
                "prior_delivery_uncertain": 1,
            },
            {
                "effect_kind": "membership",
                "membership_delivery_state": "ready",
                "prior_delivery_uncertain": 1,
            },
            {"request_ciphertext": "not-blob"},
            {"request_sha256": "r" * 32},
            {"request_sha256": b"short"},
            {"state_ciphertext": "not-blob"},
            {"state_sha256": "s" * 32},
            {"state_sha256": b"short"},
            {"created_revision": "invalid"},
            {"created_revision": -1},
            {"updated_revision": "invalid"},
            {"created_revision": 2, "updated_revision": 1},
        )
        for ordinal, changes in enumerate(invalid_changes, start=100):
            candidate = _raw_effect_row(
                effect_id=str(UUID(int=ordinal)),
                room_id=f"!invalid-{ordinal}",
            )
            candidate.update(changes)
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(_RAW_EFFECT_INSERT, candidate)

        same_membership_room = _raw_effect_row(
            effect_id="77777777-7777-7777-7777-777777777777",
            effect_kind="membership",
            room_id="!membership-ready",
            membership_delivery_state="ready",
            prior_delivery_uncertain=0,
        )
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(_RAW_EFFECT_INSERT, same_membership_room)
        for kind, effect_id in (
            ("recovery", "88888888-8888-8888-8888-888888888888"),
            ("room_hydration", "99999999-9999-9999-9999-999999999999"),
        ):
            connection.execute(
                _RAW_EFFECT_INSERT,
                _raw_effect_row(
                    effect_id=effect_id,
                    effect_kind=kind,
                    room_id="!membership-ready",
                ),
            )

        expected_indexes = {
            "NioIngestNetworkEffect_membership_room": (
                True,
                True,
                ("account_id", "room_id"),
            ),
            "NioIngestNetworkEffect_room": (
                False,
                False,
                ("account_id", "room_id"),
            ),
            "NioIngestNetworkEffect_order": (
                False,
                False,
                ("account_id", "created_revision", "effect_id"),
            ),
            "NioIngestNetworkEffect_schedulable": (
                False,
                True,
                ("account_id", "created_revision", "effect_id"),
            ),
        }
        actual_indexes = {}
        for _, name, unique, _, partial in connection.execute(
            'PRAGMA index_list("NioIngestNetworkEffect")'
        ):
            if name.startswith("sqlite_"):
                continue
            columns = tuple(
                row[2] for row in connection.execute(f'PRAGMA index_info("{name}")')
            )
            actual_indexes[name] = (bool(unique), bool(partial), columns)
        assert actual_indexes == expected_indexes


def test_journal_transition_and_port_expose_bounded_effect_crud() -> None:
    assert tuple(field.name for field in fields(JournalTransition))[-3:] == (
        "network_effect_inserts",
        "network_effect_updates",
        "network_effect_deletes",
    )
    assert {
        "load_network_effect",
        "list_network_effects",
        "list_schedulable_network_effects",
    } <= IngestionJournal.__dict__.keys()
    assert tuple(signature(IngestionJournal.load_network_effect).parameters) == (
        "self",
        "effect_id",
    )
    for method_name in ("list_network_effects", "list_schedulable_network_effects"):
        parameters = signature(IngestionJournal.__dict__[method_name]).parameters
        assert tuple(parameters) == ("self", "limit")
        assert parameters["limit"].default is parameters["limit"].empty

    with pytest.raises(TypeError, match="network_effect_inserts"):
        JournalTransition(network_effect_inserts=[])  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="network_effect_updates"):
        JournalTransition(network_effect_updates=(object(),))  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="network_effect_deletes"):
        JournalTransition(network_effect_deletes=(str(MEMBERSHIP_EFFECT_ID),))  # type: ignore[arg-type]
    with pytest.raises(ValueError, match="256"):
        JournalTransition(
            network_effect_deletes=tuple(
                UUID(int=ordinal + 1) for ordinal in range(257)
            )
        )

    effect = _membership(UUID("33333333-3333-3333-3333-333333333333"))
    with pytest.raises(ValueError, match="duplicate|overlap"):
        JournalTransition(
            network_effect_inserts=(effect,),
            network_effect_updates=(effect,),
        )
    with pytest.raises(ValueError, match="duplicate|overlap"):
        JournalTransition(
            network_effect_inserts=(effect,),
            network_effect_deletes=(effect.request.effect_id,),
        )
    for changes in (
        {"network_effect_inserts": (effect, effect)},
        {"network_effect_updates": (effect, effect)},
        {
            "network_effect_deletes": (
                effect.request.effect_id,
                effect.request.effect_id,
            )
        },
        {
            "network_effect_updates": (effect,),
            "network_effect_deletes": (effect.request.effect_id,),
        },
    ):
        with pytest.raises(ValueError, match="duplicate|overlap"):
            JournalTransition(**changes)

    inserts = tuple(
        replace(
            effect,
            request=replace(effect.request, effect_id=UUID(int=ordinal)),
        )
        for ordinal in range(1, 86)
    )
    updates = tuple(
        replace(
            effect,
            request=replace(effect.request, effect_id=UUID(int=ordinal)),
        )
        for ordinal in range(86, 171)
    )
    deletes = tuple(UUID(int=ordinal) for ordinal in range(171, 258))
    with pytest.raises(ValueError, match="256"):
        JournalTransition(
            network_effect_inserts=inserts,
            network_effect_updates=updates,
            network_effect_deletes=deletes,
        )


@pytest.mark.asyncio
async def test_all_effect_variants_round_trip_restart_and_list_as_schedulable(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
    )
    rooms = ("!hydration", "!membership", "!recovery")
    consumer = _consumer(bootstrap, rooms)
    await bootstrap.attach_consumer(consumer)
    recovery, recovery_state, recovery_lane = _recovery(bootstrap.stream_id)
    hydration = _hydration(bootstrap.stream_id)
    membership = _membership(bootstrap.stream_id)
    effects = (recovery, hydration, membership)
    expected = tuple(sorted(effects, key=lambda value: str(value.request.effect_id)))
    try:
        owner = bootstrap._journal.load_owner()
        result = bootstrap._journal.commit(
            expected_revision=owner.revision,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(
                room_states=(recovery_state,),
                room_lanes=(recovery_lane,),
                network_effect_inserts=effects,
            ),
        )

        assert result.revision == owner.revision + 1
        for effect in effects:
            assert (
                bootstrap._journal.load_network_effect(effect.request.effect_id)
                == effect
            )
        assert bootstrap._journal.list_network_effects(256) == expected
        assert bootstrap._journal.list_schedulable_network_effects(256) == expected
    finally:
        bootstrap.close()

    reopened = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        await reopened.attach_consumer(consumer)
        assert reopened._journal.list_network_effects(256) == expected
        assert reopened._journal.list_schedulable_network_effects(256) == expected
    finally:
        reopened.close()


@pytest.mark.parametrize(
    "column",
    (
        "request_ciphertext",
        "request_sha256",
        "state_ciphertext",
        "state_sha256",
    ),
)
@pytest.mark.asyncio
async def test_effect_ciphertext_or_digest_tamper_fails_closed_for_all_reads(
    tmp_path: Path,
    column: str,
) -> None:
    bootstrap, effect, _ = await _journal_with_membership_effect(tmp_path)
    journal = bootstrap._journal
    try:
        value = journal.connection.execute(
            f"SELECT {column} FROM NioIngestNetworkEffect "
            "WHERE account_id = ? AND effect_id = ?",
            (ACCOUNT_ID, str(effect.request.effect_id)),
        ).fetchone()[0]
        tampered = bytearray(value)
        tampered[-1] ^= 1
        journal.connection.execute(
            f"UPDATE NioIngestNetworkEffect SET {column} = ? "
            "WHERE account_id = ? AND effect_id = ?",
            (bytes(tampered), ACCOUNT_ID, str(effect.request.effect_id)),
        )

        for operation in (
            lambda: journal.load_network_effect(effect.request.effect_id),
            lambda: journal.list_network_effects(256),
            lambda: journal.list_schedulable_network_effects(256),
        ):
            with pytest.raises(JournalIntegrityError):
                operation()
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("column", "forged_value", "lookup_id"),
    (
        (
            "effect_id",
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
            UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        ),
        ("effect_kind", "room_hydration", MEMBERSHIP_EFFECT_ID),
        ("room_id", "!forged", MEMBERSHIP_EFFECT_ID),
        ("membership_epoch", 1, MEMBERSHIP_EFFECT_ID),
        ("attempt_ordinal", 1, MEMBERSHIP_EFFECT_ID),
        (
            "membership_delivery_state",
            "dispatched_unconfirmed",
            MEMBERSHIP_EFFECT_ID,
        ),
        ("prior_delivery_uncertain", 1, MEMBERSHIP_EFFECT_ID),
        ("created_revision", 1, MEMBERSHIP_EFFECT_ID),
        ("updated_revision", 3, MEMBERSHIP_EFFECT_ID),
    ),
)
@pytest.mark.asyncio
async def test_effect_plaintext_metadata_must_match_authenticated_envelopes(
    tmp_path: Path,
    column: str,
    forged_value: object,
    lookup_id: UUID,
) -> None:
    bootstrap, effect, _ = await _journal_with_membership_effect(tmp_path)
    journal = bootstrap._journal
    try:
        journal.connection.execute("PRAGMA ignore_check_constraints = ON")
        journal.connection.execute(
            f"UPDATE NioIngestNetworkEffect SET {column} = ? "
            "WHERE account_id = ? AND effect_id = ?",
            (forged_value, ACCOUNT_ID, str(effect.request.effect_id)),
        )

        with pytest.raises(JournalIntegrityError):
            journal.load_network_effect(lookup_id)
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("part", "mutation", "field", "value"),
    (
        ("request", "remove", "effect_kind", None),
        ("request", "extra", "extra", 1),
        ("request", "replace", "effect_kind", "foreign"),
        ("request", "replace", "membership_epoch", 0.0),
        ("state", "remove", "effect_kind", None),
        ("state", "extra", "extra", 1),
        ("state", "replace", "effect_kind", "foreign"),
        ("state", "replace", "attempt_ordinal", 0.0),
        (
            "state",
            "replace",
            "effect_id",
            "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
        ),
        ("state", "replace", "created_revision", 1),
        ("state", "replace", "updated_revision", 3),
    ),
)
@pytest.mark.asyncio
async def test_effect_envelopes_require_exact_fields_types_identity_and_revisions(
    tmp_path: Path,
    part: str,
    mutation: str,
    field: str,
    value: object,
) -> None:
    bootstrap, effect, _ = await _journal_with_membership_effect(tmp_path)
    journal = bootstrap._journal
    try:
        envelope = _effect_envelope(journal, effect.request.effect_id, part)
        if mutation == "remove":
            del envelope[field]
        else:
            envelope[field] = value
        _reseal_effect_envelope(
            journal,
            effect.request.effect_id,
            part,
            envelope,
        )

        with pytest.raises(JournalIntegrityError):
            journal.load_network_effect(effect.request.effect_id)
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("stream_id", "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        ("transport", "sliding"),
    ),
)
@pytest.mark.asyncio
async def test_resealed_effect_request_must_match_immutable_owner(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    bootstrap, effect, _ = await _journal_with_membership_effect(tmp_path)
    journal = bootstrap._journal
    try:
        envelope = _effect_envelope(journal, effect.request.effect_id, "request")
        envelope[field] = value
        _reseal_effect_envelope(
            journal,
            effect.request.effect_id,
            "request",
            envelope,
        )

        with pytest.raises(JournalIntegrityError, match="network effect"):
            journal.load_network_effect(effect.request.effect_id)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_resealed_effect_request_wraps_lone_surrogate_canonicalization(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer(bootstrap, ("!recovery",)))
    effect, state, lane = _recovery(bootstrap.stream_id)
    journal = bootstrap._journal
    try:
        journal.commit(
            expected_revision=1,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(
                room_states=(state,),
                room_lanes=(lane,),
                network_effect_inserts=(effect,),
            ),
        )
        envelope = _effect_envelope(journal, effect.request.effect_id, "request")
        envelope["from_token"] = "\ud800"
        escaped_payload = json.dumps(
            envelope,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode()
        _reseal_effect_envelope(
            journal,
            effect.request.effect_id,
            "request",
            envelope,
            payload=escaped_payload,
        )

        with pytest.raises(JournalIntegrityError, match="authenticated envelope"):
            journal.load_network_effect(effect.request.effect_id)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_resealed_effect_envelope_rejects_excessive_json_depth(
    tmp_path: Path,
) -> None:
    bootstrap, effect, _ = await _journal_with_membership_effect(tmp_path)
    journal = bootstrap._journal
    try:
        payload = b'{"effect_kind":' + (b"[" * 100_000) + b"0" + (b"]" * 100_000) + b"}"
        ciphertext, digest = journal._codec.seal(
            "NioIngestNetworkEffect.request",
            (effect.request.effect_id,),
            payload,
        )
        journal.connection.execute(
            "UPDATE NioIngestNetworkEffect SET request_ciphertext = ?, "
            "request_sha256 = ? WHERE account_id = ? AND effect_id = ?",
            (ciphertext, digest, ACCOUNT_ID, str(effect.request.effect_id)),
        )

        with pytest.raises(JournalIntegrityError, match="authenticated envelope"):
            journal.load_network_effect(effect.request.effect_id)
    finally:
        bootstrap.close()


@pytest.mark.parametrize("timeout_ms", ((1 << 53) - 1, 1 << 53, 1 << 62))
@pytest.mark.asyncio
async def test_effect_internal_envelope_integers_round_trip_with_frozen_value_range(
    tmp_path: Path,
    timeout_ms: int,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer(bootstrap, ("!membership",)))
    effect = _membership(bootstrap.stream_id)
    effect = replace(
        effect,
        request=replace(effect.request, timeout_ms=timeout_ms),
    )
    journal = bootstrap._journal
    try:
        committed = journal.commit(
            expected_revision=1,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(network_effect_inserts=(effect,)),
        )

        assert committed.revision == 2
        assert journal.load_network_effect(effect.request.effect_id) == effect
        assert journal.list_network_effects(1) == (effect,)
    finally:
        bootstrap.close()


@pytest.mark.parametrize("swap", ("rows", "domains"))
@pytest.mark.asyncio
async def test_effect_request_and_state_halves_cannot_be_swapped(
    tmp_path: Path,
    swap: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
    )
    rooms = ("!membership", "!second")
    await bootstrap.attach_consumer(_consumer(bootstrap, rooms))
    first = _membership(bootstrap.stream_id)
    second = replace(
        first,
        request=replace(
            first.request,
            effect_id=UUID("66666666-6666-4666-8666-666666666666"),
            room_id="!second",
        ),
    )
    journal = bootstrap._journal
    try:
        journal.commit(
            expected_revision=1,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(network_effect_inserts=(first, second)),
        )
        first_row = journal.connection.execute(
            "SELECT * FROM NioIngestNetworkEffect "
            "WHERE account_id = ? AND effect_id = ?",
            (ACCOUNT_ID, str(first.request.effect_id)),
        ).fetchone()
        if swap == "rows":
            journal.connection.execute(
                "UPDATE NioIngestNetworkEffect SET request_ciphertext = ?, "
                "request_sha256 = ? WHERE account_id = ? AND effect_id = ?",
                (
                    first_row["request_ciphertext"],
                    first_row["request_sha256"],
                    ACCOUNT_ID,
                    str(second.request.effect_id),
                ),
            )
            target = second.request.effect_id
        else:
            journal.connection.execute(
                "UPDATE NioIngestNetworkEffect SET state_ciphertext = ?, "
                "state_sha256 = ? WHERE account_id = ? AND effect_id = ?",
                (
                    first_row["request_ciphertext"],
                    first_row["request_sha256"],
                    ACCOUNT_ID,
                    str(first.request.effect_id),
                ),
            )
            target = first.request.effect_id

        with pytest.raises(JournalIntegrityError):
            journal.load_network_effect(target)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_same_effect_id_changed_request_collision_rejects_before_dml(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap, effect, inserted = await _journal_with_membership_effect(
        tmp_path,
        statement_observer=statements.append,
    )
    journal = bootstrap._journal
    try:
        collision = replace(
            effect,
            request=replace(effect.request, action=MembershipAction.LEAVE),
        )
        statements.clear()
        with pytest.raises(JournalIntegrityError, match="collides"):
            journal.commit(
                expected_revision=inserted.revision,
                writer_epoch=journal.writer_epoch,
                transition=JournalTransition(network_effect_inserts=(collision,)),
            )
        assert journal.load_owner().revision == inserted.revision
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize("operation", ("update", "delete"))
@pytest.mark.parametrize("corruption", ("ciphertext", "resealed"))
@pytest.mark.asyncio
async def test_effect_mutation_authenticates_existing_target_before_dml(
    tmp_path: Path,
    operation: str,
    corruption: str,
) -> None:
    statements: list[str] = []
    bootstrap, effect, inserted = await _journal_with_membership_effect(
        tmp_path,
        statement_observer=statements.append,
    )
    journal = bootstrap._journal
    try:
        if corruption == "ciphertext":
            row = journal.connection.execute(
                "SELECT state_ciphertext FROM NioIngestNetworkEffect "
                "WHERE account_id = ? AND effect_id = ?",
                (ACCOUNT_ID, str(effect.request.effect_id)),
            ).fetchone()
            ciphertext = bytearray(row["state_ciphertext"])
            ciphertext[-1] ^= 1
            journal.connection.execute(
                "UPDATE NioIngestNetworkEffect SET state_ciphertext = ? "
                "WHERE account_id = ? AND effect_id = ?",
                (bytes(ciphertext), ACCOUNT_ID, str(effect.request.effect_id)),
            )
        else:
            envelope = _effect_envelope(
                journal,
                effect.request.effect_id,
                "state",
            )
            envelope["extra"] = True
            _reseal_effect_envelope(
                journal,
                effect.request.effect_id,
                "state",
                envelope,
            )
        raw_before = tuple(
            journal.connection.execute(
                "SELECT * FROM NioIngestNetworkEffect "
                "WHERE account_id = ? AND effect_id = ?",
                (ACCOUNT_ID, str(effect.request.effect_id)),
            ).fetchone()
        )
        transition = (
            JournalTransition(network_effect_updates=(effect,))
            if operation == "update"
            else JournalTransition(
                network_effect_deletes=(effect.request.effect_id,),
            )
        )
        statements.clear()

        with pytest.raises(JournalIntegrityError):
            journal.commit(
                expected_revision=inserted.revision,
                writer_epoch=journal.writer_epoch,
                transition=transition,
            )
        assert journal.load_owner().revision == inserted.revision
        assert (
            tuple(
                journal.connection.execute(
                    "SELECT * FROM NioIngestNetworkEffect "
                    "WHERE account_id = ? AND effect_id = ?",
                    (ACCOUNT_ID, str(effect.request.effect_id)),
                ).fetchone()
            )
            == raw_before
        )
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "case",
    (
        "stream",
        "transport",
        "room",
        "epoch",
        "gap",
        "page",
        "from_token",
        "to_token",
    ),
)
@pytest.mark.asyncio
async def test_recovery_insert_requires_the_exact_owner_and_gap_link_before_dml(
    tmp_path: Path,
    case: str,
) -> None:
    statements: list[str] = []
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
        statement_observer=statements.append,
    )
    await bootstrap.attach_consumer(_consumer(bootstrap, ("!recovery",)))
    effect, state, lane = _recovery(bootstrap.stream_id)
    request = effect.request
    if case == "stream":
        request = replace(request, stream_id=UUID(int=9876))
    elif case == "transport":
        request = replace(request, transport=TransportKind.SLIDING)
    elif case == "room":
        request = replace(request, room_id="!other")
    elif case == "epoch":
        request = replace(request, membership_epoch=1)
    elif case == "gap":
        gap_id = UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
        request = replace(
            request,
            effect_id=uuid5(gap_id, "page:1"),
            gap_id=gap_id,
        )
    elif case == "page":
        request = replace(
            request,
            effect_id=uuid5(request.gap_id, "page:2"),
            page_ordinal=2,
        )
    elif case == "from_token":
        request = replace(request, from_token="wrong-cursor")
    else:
        request = replace(request, to_token="wrong-target")
    candidate = replace(effect, request=request)
    lane = replace(
        lane,
        recovery_gap=replace(
            lane.recovery_gap,
            in_flight_effect_id=request.effect_id,
        ),
    )
    journal = bootstrap._journal
    try:
        statements.clear()
        with pytest.raises(JournalIntegrityError):
            journal.commit(
                expected_revision=1,
                writer_epoch=journal.writer_epoch,
                transition=JournalTransition(
                    room_states=(state,),
                    room_lanes=(lane,),
                    network_effect_inserts=(candidate,),
                ),
            )
        assert journal.load_owner().revision == 1
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "case",
    ("hydration_nonpending", "membership_stale_epoch", "duplicate_membership"),
)
@pytest.mark.asyncio
async def test_nonrecovery_effects_require_exact_current_room_links_before_dml(
    tmp_path: Path,
    case: str,
) -> None:
    statements: list[str] = []
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
        statement_observer=statements.append,
    )
    room_id = "!hydration" if case == "hydration_nonpending" else "!membership"
    await bootstrap.attach_consumer(_consumer(bootstrap, (room_id,)))
    journal = bootstrap._journal
    if case == "hydration_nonpending":
        candidate = _hydration(bootstrap.stream_id)
        state = journal.load_rooms(frozenset({room_id}))[room_id].state
        transition = JournalTransition(
            room_states=(
                replace(state, hydration_status=RoomHydrationStatus.UNAVAILABLE),
            ),
            network_effect_inserts=(candidate,),
        )
    elif case == "membership_stale_epoch":
        candidate = _membership(bootstrap.stream_id)
        transition = JournalTransition(
            network_effect_inserts=(
                replace(
                    candidate,
                    request=replace(candidate.request, membership_epoch=1),
                ),
            ),
        )
    else:
        first = _membership(bootstrap.stream_id)
        second = replace(
            first,
            request=replace(first.request, effect_id=UUID(int=9876)),
        )
        transition = JournalTransition(network_effect_inserts=(first, second))
    try:
        statements.clear()
        with pytest.raises(JournalIntegrityError):
            journal.commit(
                expected_revision=1,
                writer_epoch=journal.writer_epoch,
                transition=transition,
            )
        assert journal.load_owner().revision == 1
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_existing_membership_room_collision_rejects_before_dml(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap, first, inserted = await _journal_with_membership_effect(
        tmp_path,
        statement_observer=statements.append,
    )
    journal = bootstrap._journal
    second = replace(
        first,
        request=replace(first.request, effect_id=UUID(int=9876)),
    )
    try:
        statements.clear()
        with pytest.raises(JournalIntegrityError, match="one unresolved membership"):
            journal.commit(
                expected_revision=inserted.revision,
                writer_epoch=journal.writer_epoch,
                transition=JournalTransition(network_effect_inserts=(second,)),
            )
        assert journal.load_owner().revision == inserted.revision
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize("limit", (True, 0, -1, 257))
def test_network_effect_lists_require_an_exact_bounded_limit(
    tmp_path: Path,
    limit: object,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
    )
    try:
        bootstrap._journal.attach_consumer_step(_consumer(bootstrap, ()))
        for method_name in (
            "list_network_effects",
            "list_schedulable_network_effects",
        ):
            with pytest.raises((TypeError, ValueError), match="limit"):
                getattr(bootstrap._journal, method_name)(limit)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_effect_lists_use_bounded_ordered_indexes_and_created_order(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
        statement_observer=statements.append,
    )
    room_ids = tuple(f"!listed-{ordinal:02d}" for ordinal in range(65))
    consumer = _consumer(bootstrap, room_ids)
    await bootstrap.attach_consumer(consumer)
    base = _membership(bootstrap.stream_id)
    first_generation = tuple(
        replace(
            base,
            request=replace(
                base.request,
                effect_id=UUID(int=1_000 + ordinal),
                room_id=room_id,
            ),
        )
        for ordinal, room_id in enumerate(room_ids[:64])
    )
    final = replace(
        base,
        request=replace(
            base.request,
            effect_id=UUID(int=1),
            room_id=room_ids[-1],
        ),
    )
    dispatched = tuple(
        replace(
            effect,
            attempt_ordinal=1,
            membership_delivery_state=(MembershipDeliveryState.DISPATCHED_UNCONFIRMED),
        )
        for effect in first_generation
    )
    journal = bootstrap._journal
    try:
        owner = journal.load_owner()
        inserted = journal.commit(
            expected_revision=owner.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(
                network_effect_inserts=first_generation,
            ),
        )
        updated = journal.commit(
            expected_revision=inserted.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(
                network_effect_inserts=(final,),
                network_effect_updates=dispatched,
            ),
        )

        expected = (
            *sorted(dispatched, key=lambda effect: str(effect.request.effect_id)),
            final,
        )
        assert journal.list_network_effects(256) == expected
        assert journal.list_schedulable_network_effects(1) == (final,)
        rows = tuple(
            journal.connection.execute(
                "SELECT effect_id, created_revision, updated_revision "
                "FROM NioIngestNetworkEffect WHERE account_id = ? "
                "ORDER BY created_revision, effect_id",
                (ACCOUNT_ID,),
            )
        )
        assert all(row[1] == inserted.revision for row in rows[:-1])
        assert all(row[2] == updated.revision for row in rows)
        assert tuple(rows[-1]) == (
            str(final.request.effect_id),
            updated.revision,
            updated.revision,
        )

        list_plan = tuple(
            row[3]
            for row in journal.connection.execute(
                "EXPLAIN QUERY PLAN SELECT * FROM NioIngestNetworkEffect "
                "WHERE account_id = ? ORDER BY created_revision, effect_id LIMIT ?",
                (ACCOUNT_ID, 1),
            )
        )
        schedulable_plan = tuple(
            row[3]
            for row in journal.connection.execute(
                "EXPLAIN QUERY PLAN SELECT * FROM NioIngestNetworkEffect "
                "WHERE account_id = ? AND (effect_kind != 'membership' "
                "OR membership_delivery_state = 'ready') "
                "ORDER BY created_revision, effect_id LIMIT ?",
                (ACCOUNT_ID, 1),
            )
        )
        assert any("NioIngestNetworkEffect_order" in detail for detail in list_plan)
        assert any(
            "NioIngestNetworkEffect_schedulable" in detail
            for detail in schedulable_plan
        )
        assert not any(
            "TEMP B-TREE" in detail for detail in (*list_plan, *schedulable_plan)
        )

        statements.clear()
        assert journal.list_network_effects(1) == (expected[0],)
        main_selects = tuple(
            statement
            for statement in statements
            if statement.startswith("SELECT * FROM NioIngestNetworkEffect ")
            and "ORDER BY created_revision, effect_id LIMIT 1" in statement
        )
        assert len(main_selects) == 1
    finally:
        bootstrap.close()

    reopened = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
    )
    try:
        await reopened.attach_consumer(consumer)
        assert reopened._journal.list_network_effects(1) == (expected[0],)
        assert reopened._journal.list_network_effects(256) == expected
        assert reopened._journal.list_schedulable_network_effects(1) == (final,)
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_membership_state_updates_filter_scheduler_and_exact_noop_is_byte_stable(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer(bootstrap, ("!membership",)))
    initial = _membership(bootstrap.stream_id)
    journal = bootstrap._journal
    try:
        inserted = journal.commit(
            expected_revision=1,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(network_effect_inserts=(initial,)),
        )
        inserted_row = _effect_row_bytes(journal, MEMBERSHIP_EFFECT_ID)
        stored_row = journal.connection.execute(
            "SELECT * FROM NioIngestNetworkEffect "
            "WHERE account_id = ? AND effect_id = ?",
            (ACCOUNT_ID, str(MEMBERSHIP_EFFECT_ID)),
        ).fetchone()
        request_payload = _canonical_membership_request_envelope(initial.request)
        assert (
            bytes(stored_row["request_sha256"])
            == hashlib.sha256(request_payload).digest()
        )
        assert (
            journal._open_payload(
                "NioIngestNetworkEffect.request",
                (MEMBERSHIP_EFFECT_ID,),
                stored_row,
                "request",
            )
            == request_payload
        )
        dispatched = replace(
            initial,
            attempt_ordinal=1,
            membership_delivery_state=(MembershipDeliveryState.DISPATCHED_UNCONFIRMED),
        )
        updated = journal.commit(
            expected_revision=inserted.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(network_effect_updates=(dispatched,)),
        )
        dispatched_row = _effect_row_bytes(journal, MEMBERSHIP_EFFECT_ID)

        assert updated.revision == inserted.revision + 1
        assert journal.load_network_effect(MEMBERSHIP_EFFECT_ID) == dispatched
        assert journal.list_network_effects(256) == (dispatched,)
        assert journal.list_schedulable_network_effects(256) == ()
        assert dispatched_row[:2] == inserted_row[:2]
        assert dispatched_row[4] == inserted_row[4]
        assert dispatched_row[2:4] != inserted_row[2:4]
        assert dispatched_row[5] == updated.revision

        no_op = journal.commit(
            expected_revision=updated.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(network_effect_updates=(dispatched,)),
        )
        assert no_op.revision == updated.revision
        assert journal.load_owner().revision == updated.revision
        assert _effect_row_bytes(journal, MEMBERSHIP_EFFECT_ID) == dispatched_row

        ready = replace(
            dispatched,
            membership_delivery_state=MembershipDeliveryState.READY,
        )
        retried = journal.commit(
            expected_revision=no_op.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(network_effect_updates=(ready,)),
        )
        assert retried.revision == no_op.revision + 1
        assert journal.list_schedulable_network_effects(256) == (ready,)
        assert _effect_row_bytes(journal, MEMBERSHIP_EFFECT_ID)[:2] == inserted_row[:2]
    finally:
        bootstrap.close()


def test_generic_effect_update_edge_matrix_is_exact_and_prior_immutable() -> None:
    base = _membership(UUID("33333333-3333-4333-8333-333333333333"))
    ready_1 = replace(base, attempt_ordinal=1)
    ready_1_uncertain = replace(ready_1, prior_delivery_uncertain=True)
    dispatched_1 = replace(
        base,
        attempt_ordinal=1,
        membership_delivery_state=MembershipDeliveryState.DISPATCHED_UNCONFIRMED,
    )
    dispatched_2 = replace(dispatched_1, attempt_ordinal=2)
    dispatched_2_uncertain = replace(
        dispatched_2,
        prior_delivery_uncertain=True,
    )

    for stored, proposed, changed in (
        (base, base, False),
        (base, dispatched_1, True),
        (ready_1, dispatched_2, True),
        (dispatched_1, ready_1, True),
        (ready_1_uncertain, dispatched_2_uncertain, True),
    ):
        assert (
            NetworkEffectRows._validate_network_effect_update_edge(
                stored,
                proposed,
            )
            is changed
        )

    invalid_edges = (
        (base, dispatched_2),
        (base, ready_1),
        (dispatched_1, base),
        (dispatched_1, replace(ready_1, prior_delivery_uncertain=True)),
        (ready_1_uncertain, dispatched_2),
        (dispatched_2_uncertain, ready_1_uncertain),
        (dispatched_1, dispatched_2),
    )
    for stored, proposed in invalid_edges:
        with pytest.raises(JournalIntegrityError, match="transition|uncertainty"):
            NetworkEffectRows._validate_network_effect_update_edge(
                stored,
                proposed,
            )


@pytest.mark.parametrize("operation", ("insert", "update"))
@pytest.mark.asyncio
async def test_effect_envelopes_are_prepared_before_meta_cas(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    operation: str,
) -> None:
    statements: list[str] = []
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
        statement_observer=statements.append,
    )
    await bootstrap.attach_consumer(_consumer(bootstrap, ("!membership",)))
    effect = _membership(bootstrap.stream_id)
    journal = bootstrap._journal
    try:
        expected_revision = 1
        if operation == "update":
            inserted = journal.commit(
                expected_revision=expected_revision,
                writer_epoch=journal.writer_epoch,
                transition=JournalTransition(network_effect_inserts=(effect,)),
            )
            expected_revision = inserted.revision
            effect = replace(
                effect,
                attempt_ordinal=1,
                membership_delivery_state=(
                    MembershipDeliveryState.DISPATCHED_UNCONFIRMED
                ),
            )
        original_seal = type(journal._codec).seal

        def fail_state_seal(codec, table, primary_key, payload):
            if table == "NioIngestNetworkEffect.state":
                raise ValueError("synthetic envelope failure")
            return original_seal(codec, table, primary_key, payload)

        monkeypatch.setattr(type(journal._codec), "seal", fail_state_seal)
        transition = JournalTransition(**{f"network_effect_{operation}s": (effect,)})
        statements.clear()

        with pytest.raises(JournalIntegrityError, match="envelope"):
            journal.commit(
                expected_revision=expected_revision,
                writer_epoch=journal.writer_epoch,
                transition=transition,
            )
        assert journal.load_owner().revision == expected_revision
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize("effect_count", (1, 256))
@pytest.mark.asyncio
async def test_effect_insert_dml_is_chunked_with_one_transaction(
    tmp_path: Path,
    effect_count: int,
) -> None:
    statements: list[str] = []
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
        statement_observer=statements.append,
    )
    room_ids = tuple(f"!effect-{ordinal:03d}" for ordinal in range(effect_count))
    await bootstrap.attach_consumer(_consumer(bootstrap, room_ids))
    base = _membership(bootstrap.stream_id)
    effects = tuple(
        replace(
            base,
            request=replace(
                base.request,
                effect_id=UUID(int=ordinal + 1),
                room_id=room_id,
            ),
        )
        for ordinal, room_id in enumerate(room_ids)
    )
    journal = bootstrap._journal
    try:
        owner = journal.load_owner()
        statements.clear()
        committed = journal.commit(
            expected_revision=owner.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(network_effect_inserts=effects),
        )

        effect_dml = tuple(
            statement
            for statement in statements
            if statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            and "NioIngestNetworkEffect" in statement
        )
        effect_id_selects = tuple(
            statement
            for statement in statements
            if "SELECT * FROM NioIngestNetworkEffect" in statement
            and "effect_id IN" in statement
        )
        room_id_selects = tuple(
            statement
            for statement in statements
            if "SELECT * FROM NioIngestNetworkEffect" in statement
            and "room_id IN" in statement
        )
        assert committed.revision == owner.revision + 1
        assert sum(statement == "BEGIN IMMEDIATE" for statement in statements) == 1
        assert sum(statement == "COMMIT" for statement in statements) == 1
        assert len(effect_dml) == math.ceil(effect_count / 32)
        assert len(effect_dml) <= 10
        assert len(effect_id_selects) == math.ceil(effect_count / 32)
        assert len(room_id_selects) == math.ceil(effect_count / 32)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_combined_256_effect_mutations_use_at_most_ten_effect_dml(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
        statement_observer=statements.append,
    )
    room_ids = tuple(f"!effect-{ordinal:03d}" for ordinal in range(342))
    await bootstrap.attach_consumer(_consumer(bootstrap, room_ids))
    base = _membership(bootstrap.stream_id)

    def effect(ordinal: int) -> PersistedNetworkEffect:
        return replace(
            base,
            request=replace(
                base.request,
                effect_id=UUID(int=ordinal + 1),
                room_id=room_ids[ordinal],
            ),
        )

    existing = tuple(effect(ordinal) for ordinal in range(256))
    journal = bootstrap._journal
    try:
        owner = journal.load_owner()
        inserted = journal.commit(
            expected_revision=owner.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(network_effect_inserts=existing),
        )
        updates = tuple(
            replace(
                candidate,
                attempt_ordinal=1,
                membership_delivery_state=(
                    MembershipDeliveryState.DISPATCHED_UNCONFIRMED
                ),
            )
            for candidate in existing[:85]
        )
        deletes = tuple(candidate.request.effect_id for candidate in existing[85:170])
        inserts = tuple(effect(ordinal) for ordinal in range(256, 342))
        statements.clear()
        committed = journal.commit(
            expected_revision=inserted.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(
                network_effect_inserts=inserts,
                network_effect_updates=updates,
                network_effect_deletes=deletes,
            ),
        )

        effect_dml = tuple(
            statement
            for statement in statements
            if statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            and "NioIngestNetworkEffect" in statement
        )
        effect_id_selects = tuple(
            statement
            for statement in statements
            if "SELECT * FROM NioIngestNetworkEffect" in statement
            and "effect_id IN" in statement
        )
        room_id_selects = tuple(
            statement
            for statement in statements
            if "SELECT * FROM NioIngestNetworkEffect" in statement
            and "room_id IN" in statement
        )
        assert committed.revision == inserted.revision + 1
        assert sum(statement == "BEGIN IMMEDIATE" for statement in statements) == 1
        assert sum(statement == "COMMIT" for statement in statements) == 1
        assert len(effect_dml) == 9
        assert len(effect_dml) <= 10
        assert len(effect_id_selects) <= 8
        assert len(room_id_selects) <= 8
        assert len(effect_id_selects) + len(room_id_selects) <= 16
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_effect_updates_reject_absent_immutable_or_invalid_edges_before_dml(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
        statement_observer=statements.append,
    )
    rooms = ("!hydration", "!membership", "!recovery")
    await bootstrap.attach_consumer(_consumer(bootstrap, rooms))
    recovery, recovery_state, recovery_lane = _recovery(bootstrap.stream_id)
    hydration = _hydration(bootstrap.stream_id)
    membership = _membership(bootstrap.stream_id)
    journal = bootstrap._journal
    try:
        inserted = journal.commit(
            expected_revision=1,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(
                room_states=(recovery_state,),
                room_lanes=(recovery_lane,),
                network_effect_inserts=(recovery, hydration, membership),
            ),
        )
        before_rows = tuple(
            journal.connection.execute(
                "SELECT * FROM NioIngestNetworkEffect ORDER BY effect_id"
            )
        )
        candidates = (
            replace(
                membership,
                attempt_ordinal=2,
                membership_delivery_state=(
                    MembershipDeliveryState.DISPATCHED_UNCONFIRMED
                ),
            ),
            replace(membership, attempt_ordinal=1),
            replace(membership, attempt_ordinal=1, prior_delivery_uncertain=True),
            replace(
                membership,
                request=replace(membership.request, action=MembershipAction.LEAVE),
            ),
            replace(
                membership,
                request=replace(membership.request, request_body=b'{"reason":"other"}'),
            ),
            replace(
                membership,
                request=replace(membership.request, effect_id=UUID(int=9876)),
            ),
            replace(hydration, request=replace(hydration.request, timeout_ms=1)),
            replace(recovery, request=replace(recovery.request, limit=1)),
        )
        for candidate in candidates:
            statements.clear()
            with pytest.raises(JournalIntegrityError):
                journal.commit(
                    expected_revision=inserted.revision,
                    writer_epoch=journal.writer_epoch,
                    transition=JournalTransition(network_effect_updates=(candidate,)),
                )
            assert journal.load_owner().revision == inserted.revision
            assert not any(
                statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
                for statement in statements
            )
            assert (
                tuple(
                    journal.connection.execute(
                        "SELECT * FROM NioIngestNetworkEffect ORDER BY effect_id"
                    )
                )
                == before_rows
            )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_membership_insert_accepts_only_initial_delivery_state(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer(bootstrap, ("!membership",)))
    initial = _membership(bootstrap.stream_id)
    later_states = (
        replace(initial, attempt_ordinal=1),
        replace(
            initial,
            attempt_ordinal=1,
            membership_delivery_state=(MembershipDeliveryState.DISPATCHED_UNCONFIRMED),
        ),
        replace(initial, attempt_ordinal=1, prior_delivery_uncertain=True),
        replace(
            initial,
            attempt_ordinal=2,
            membership_delivery_state=(MembershipDeliveryState.DISPATCHED_UNCONFIRMED),
            prior_delivery_uncertain=True,
        ),
    )
    try:
        for effect in later_states:
            with pytest.raises(JournalIntegrityError, match="start READY"):
                bootstrap._journal.commit(
                    expected_revision=1,
                    writer_epoch=bootstrap._journal.writer_epoch,
                    transition=JournalTransition(network_effect_inserts=(effect,)),
                )
            assert bootstrap._journal.load_owner().revision == 1
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_effect_deletes_require_exact_owner_closure_in_same_transition(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
        statement_observer=statements.append,
    )
    rooms = ("!hydration", "!membership", "!recovery")
    await bootstrap.attach_consumer(_consumer(bootstrap, rooms))
    recovery, recovery_state, recovery_lane = _recovery(bootstrap.stream_id)
    hydration = _hydration(bootstrap.stream_id)
    membership = _membership(bootstrap.stream_id)
    journal = bootstrap._journal
    try:
        inserted = journal.commit(
            expected_revision=1,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(
                room_states=(recovery_state,),
                room_lanes=(recovery_lane,),
                network_effect_inserts=(recovery, hydration, membership),
            ),
        )
        for effect in (recovery, hydration):
            statements.clear()
            with pytest.raises(JournalIntegrityError):
                journal.commit(
                    expected_revision=inserted.revision,
                    writer_epoch=journal.writer_epoch,
                    transition=JournalTransition(
                        network_effect_deletes=(effect.request.effect_id,),
                    ),
                )
            assert journal.load_owner().revision == inserted.revision
            assert not any(
                statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
                for statement in statements
            )

        membership_deleted = journal.commit(
            expected_revision=inserted.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(
                network_effect_deletes=(membership.request.effect_id,),
            ),
        )
        assert journal.load_network_effect(membership.request.effect_id) is None

        aggregates = journal.load_rooms(frozenset({"!hydration", "!recovery"}))
        hydration_state = replace(
            aggregates["!hydration"].state,
            hydration_status=RoomHydrationStatus.UNAVAILABLE,
        )
        recovery_lane_closed = replace(
            aggregates["!recovery"].active_lane,
            release_phase=ReleasePhase.IDLE,
            recovery_gap=None,
        )
        closed = journal.commit(
            expected_revision=membership_deleted.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(
                room_states=(hydration_state,),
                room_lanes=(recovery_lane_closed,),
                network_effect_deletes=(
                    hydration.request.effect_id,
                    recovery.request.effect_id,
                ),
            ),
        )
        assert closed.revision == membership_deleted.revision + 1
        assert journal.list_network_effects(256) == ()

        statements.clear()
        with pytest.raises(JournalIntegrityError, match="absent|missing"):
            journal.commit(
                expected_revision=closed.revision,
                writer_epoch=journal.writer_epoch,
                transition=JournalTransition(
                    network_effect_deletes=(UUID(int=9876),),
                ),
            )
        assert journal.load_owner().revision == closed.revision
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize("final_state_first", (True, False))
@pytest.mark.asyncio
async def test_hydration_delete_rejects_duplicate_room_state_keys_before_dml(
    tmp_path: Path,
    final_state_first: bool,
) -> None:
    statements: list[str] = []
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
        statement_observer=statements.append,
    )
    await bootstrap.attach_consumer(_consumer(bootstrap, ("!hydration",)))
    hydration = _hydration(bootstrap.stream_id)
    journal = bootstrap._journal
    try:
        inserted = journal.commit(
            expected_revision=1,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(network_effect_inserts=(hydration,)),
        )
        pending = journal.load_rooms(frozenset({"!hydration"}))["!hydration"].state
        unavailable = replace(
            pending,
            hydration_status=RoomHydrationStatus.UNAVAILABLE,
        )
        room_states = (
            (unavailable, pending) if final_state_first else (pending, unavailable)
        )
        statements.clear()

        with pytest.raises(JournalIntegrityError, match="duplicate.*room state"):
            journal.commit(
                expected_revision=inserted.revision,
                writer_epoch=journal.writer_epoch,
                transition=JournalTransition(
                    room_states=room_states,
                    network_effect_deletes=(hydration.request.effect_id,),
                ),
            )
        assert journal.load_owner().revision == inserted.revision
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize("operation", ("insert", "update"))
@pytest.mark.asyncio
async def test_effect_noop_rejects_persisted_writer_epoch_drift_before_dml(
    tmp_path: Path,
    operation: str,
) -> None:
    statements: list[str] = []
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
        statement_observer=statements.append,
    )
    await bootstrap.attach_consumer(_consumer(bootstrap, ("!membership",)))
    effect = _membership(bootstrap.stream_id)
    journal = bootstrap._journal
    try:
        inserted = journal.commit(
            expected_revision=1,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(network_effect_inserts=(effect,)),
        )
        journal.connection.execute(
            "UPDATE NioIngestMeta SET writer_epoch = ? WHERE account_id = ?",
            (str(UUID(int=9876)), ACCOUNT_ID),
        )
        transition = JournalTransition(
            **{
                f"network_effect_{operation}s": (effect,),
            }
        )
        statements.clear()

        with pytest.raises(JournalConflictError, match="writer_epoch"):
            journal.commit(
                expected_revision=inserted.revision,
                writer_epoch=journal.writer_epoch,
                transition=transition,
            )
        assert journal.load_owner().revision == inserted.revision
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_source_only_reset_preserves_every_effect_row_byte_for_byte(
    tmp_path: Path,
) -> None:
    source_config = _sliding_source()
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=source_config,
        pickle_key="secret",
        database_name="journal.db",
    )
    rooms = ("!hydration", "!membership", "!recovery")
    consumer = _consumer(bootstrap, rooms)
    await bootstrap.attach_consumer(consumer)
    recovery, recovery_state, recovery_lane = _recovery(
        bootstrap.stream_id,
        transport=TransportKind.SLIDING,
    )
    effects = (
        recovery,
        _hydration(bootstrap.stream_id, transport=TransportKind.SLIDING),
        _membership(bootstrap.stream_id, transport=TransportKind.SLIDING),
    )
    journal = bootstrap._journal
    try:
        inserted = journal.commit(
            expected_revision=1,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(
                room_states=(recovery_state,),
                room_lanes=(recovery_lane,),
                network_effect_inserts=effects,
            ),
        )
        before = tuple(
            tuple(row)
            for row in journal.connection.execute(
                "SELECT * FROM NioIngestNetworkEffect ORDER BY effect_id"
            )
        )
        membership = effects[2]
        assert type(membership.request) is MembershipRequest
        membership_row = journal.connection.execute(
            "SELECT request_sha256 FROM NioIngestNetworkEffect "
            "WHERE account_id = ? AND effect_id = ?",
            (ACCOUNT_ID, str(membership.request.effect_id)),
        ).fetchone()
        expected_membership_digest = hashlib.sha256(
            _canonical_membership_request_envelope(membership.request)
        ).digest()
        assert bytes(membership_row["request_sha256"]) == expected_membership_digest
        source = journal.load_source()
        cursor = _sliding_cursor_from_json(source.cursor_json)
        reset_cursor = replace(
            cursor,
            pos="next-pos",
            to_device_since="next-to-device",
            connection_instance=UUID(int=9876),
        )
        reset = SourceState(
            source.source_epoch + 1,
            source.transport_kind,
            canonical_sliding_cursor(reset_cursor),
            source.next_request_id + 1,
            source.active,
        )
        committed = journal.commit(
            expected_revision=inserted.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(source_state=reset),
        )
        after = tuple(
            tuple(row)
            for row in journal.connection.execute(
                "SELECT * FROM NioIngestNetworkEffect ORDER BY effect_id"
            )
        )

        assert committed.revision == inserted.revision + 1
        assert after == before
        assert journal.list_network_effects(256) == tuple(
            sorted(effects, key=lambda effect: str(effect.request.effect_id))
        )
    finally:
        bootstrap.close()

    reopened = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=source_config,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        await reopened.attach_consumer(consumer)
        after_reopen = tuple(
            tuple(row)
            for row in reopened._journal.connection.execute(
                "SELECT * FROM NioIngestNetworkEffect ORDER BY effect_id"
            )
        )
        assert after_reopen == before
        assert reopened._journal.load_source() == reset
        reopened_membership_digest = reopened._journal.connection.execute(
            "SELECT request_sha256 FROM NioIngestNetworkEffect "
            "WHERE account_id = ? AND effect_id = ?",
            (ACCOUNT_ID, str(membership.request.effect_id)),
        ).fetchone()[0]
        assert bytes(reopened_membership_digest) == expected_membership_digest
        assert reopened._journal.list_network_effects(256) == tuple(
            sorted(effects, key=lambda effect: str(effect.request.effect_id))
        )
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_recovery_target_extension_preserves_frozen_request_and_restart(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer(bootstrap, ("!recovery",))
    await bootstrap.attach_consumer(consumer)
    effect, state, lane = _recovery(bootstrap.stream_id)
    journal = bootstrap._journal
    try:
        inserted = journal.commit(
            expected_revision=1,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(
                room_states=(state,),
                room_lanes=(lane,),
                network_effect_inserts=(effect,),
            ),
        )
        before = _effect_row_bytes(journal, effect.request.effect_id)
        extended_lane = replace(
            lane,
            recovery_gap=replace(lane.recovery_gap, target_token="later-target"),
        )
        extended = journal.commit(
            expected_revision=inserted.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(room_lanes=(extended_lane,)),
        )

        assert _effect_row_bytes(journal, effect.request.effect_id) == before
        retried = journal.commit(
            expected_revision=extended.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(network_effect_inserts=(effect,)),
        )
        assert retried.revision == extended.revision
        assert _effect_row_bytes(journal, effect.request.effect_id) == before
    finally:
        bootstrap.close()

    reopened = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        await reopened.attach_consumer(consumer)
        assert reopened._journal.load_network_effect(effect.request.effect_id) == effect
        assert (
            reopened._journal.load_rooms(frozenset({"!recovery"}))[
                "!recovery"
            ].active_lane.recovery_gap.target_token
            == "later-target"
        )
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_recovery_page_advance_preserves_historical_lane_provenance(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer(bootstrap, ("!recovery",))
    await bootstrap.attach_consumer(consumer)
    first, state, lane = _recovery(bootstrap.stream_id)
    recovered = EventRecord(
        "$recovered-record",
        RecordKind.TIMELINE,
        RecordOrigin(TransportKind.CLASSIC, 4, 9, 3),
        "!recovery",
        0,
        1,
        "$recovered-event",
        TimelineEventProvenance.RECOVERED,
        b'{"type":"m.room.message"}',
        None,
    )
    lane_record = LaneRecord(
        LaneRecordKey(
            "!recovery",
            0,
            LaneRecordSection.RECOVERED,
            1,
            0,
        ),
        recovered,
        None,
        first.request.effect_id,
        len(_canonical_json(_record_to_dict(recovered))),
    )
    journal = bootstrap._journal
    try:
        inserted = journal.commit(
            expected_revision=1,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(
                room_states=(state,),
                room_lanes=(lane,),
                lane_record_inserts=(lane_record,),
                network_effect_inserts=(first,),
            ),
        )
        second_effect_id = uuid5(first.request.gap_id, "page:2")
        advanced_gap = replace(
            lane.recovery_gap,
            cursor_token="cursor-2",
            seen_cursor_tokens=("start-token", "cursor-1", "cursor-2"),
            pages_committed=2,
            in_flight_effect_id=second_effect_id,
        )
        second = PersistedNetworkEffect(
            RecoveryRequest(
                second_effect_id,
                bootstrap.stream_id,
                TransportKind.CLASSIC,
                "!recovery",
                0,
                first.request.gap_id,
                2,
                "cursor-2",
                advanced_gap.target_token,
                first.request.limit,
                first.request.timeout_ms,
            ),
            0,
            None,
            None,
        )
        advanced = journal.commit(
            expected_revision=inserted.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(
                room_lanes=(replace(lane, recovery_gap=advanced_gap),),
                network_effect_inserts=(second,),
                network_effect_deletes=(first.request.effect_id,),
            ),
        )

        assert advanced.revision == inserted.revision + 1
        assert journal.load_network_effect(first.request.effect_id) is None
        assert journal.load_network_effect(second.request.effect_id) == second
        assert journal.load_lane_record(lane_record.key) == lane_record
    finally:
        bootstrap.close()

    reopened = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        await reopened.attach_consumer(consumer)
        assert reopened._journal.load_network_effect(first.request.effect_id) is None
        assert reopened._journal.load_network_effect(second.request.effect_id) == second
        assert reopened._journal.load_lane_record(lane_record.key) == lane_record
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_recovery_pointer_cannot_be_shared_across_rooms(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
        statement_observer=statements.append,
    )
    rooms = ("!room-a", "!room-b")
    await bootstrap.attach_consumer(_consumer(bootstrap, rooms))
    effect_a, state_a, lane_a = _recovery(
        bootstrap.stream_id,
        room_id="!room-a",
        gap_id=UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
    )
    _, state_b, lane_b = _recovery(
        bootstrap.stream_id,
        room_id="!room-b",
        gap_id=UUID("bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"),
    )
    shared_lane_b = replace(
        lane_b,
        recovery_gap=replace(
            lane_b.recovery_gap,
            in_flight_effect_id=effect_a.request.effect_id,
        ),
    )
    try:
        statements.clear()
        with pytest.raises(JournalIntegrityError, match="network effect"):
            bootstrap._journal.commit(
                expected_revision=1,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    room_states=(state_a, state_b),
                    room_lanes=(lane_a, shared_lane_b),
                    network_effect_inserts=(effect_a,),
                ),
            )
        assert bootstrap._journal.load_owner().revision == 1
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_room_load_rejects_a_missing_recovery_effect_pointer(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer(bootstrap, ("!recovery",)))
    effect, state, lane = _recovery(bootstrap.stream_id)
    try:
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(
                room_states=(state,),
                room_lanes=(lane,),
                network_effect_inserts=(effect,),
            ),
        )
        bootstrap._journal.connection.execute(
            "DELETE FROM NioIngestNetworkEffect WHERE account_id = ? AND effect_id = ?",
            (ACCOUNT_ID, str(effect.request.effect_id)),
        )

        with pytest.raises(JournalIntegrityError, match="pointer"):
            bootstrap._journal.load_rooms(frozenset({"!recovery"}))
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_transition_cannot_launder_a_stale_current_recovery_link(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
        statement_observer=statements.append,
    )
    await bootstrap.attach_consumer(_consumer(bootstrap, ("!recovery",)))
    effect, state, lane = _recovery(bootstrap.stream_id)
    journal = bootstrap._journal
    try:
        inserted = journal.commit(
            expected_revision=1,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(
                room_states=(state,),
                room_lanes=(lane,),
                network_effect_inserts=(effect,),
            ),
        )
        stale_gap = replace(
            lane.recovery_gap,
            cursor_token="cursor-2",
            seen_cursor_tokens=("start-token", "cursor-1", "cursor-2"),
            pages_committed=2,
        )
        journal._write_room_lane(
            replace(lane, recovery_gap=stale_gap),
            inserted.revision,
            TransportKind.CLASSIC,
        )
        statements.clear()

        with pytest.raises(JournalIntegrityError, match="recovery"):
            journal.commit(
                expected_revision=inserted.revision,
                writer_epoch=journal.writer_epoch,
                transition=JournalTransition(
                    room_lanes=(
                        replace(
                            lane, release_phase=ReleasePhase.IDLE, recovery_gap=None
                        ),
                    ),
                    network_effect_deletes=(effect.request.effect_id,),
                ),
            )
        assert journal.load_owner().revision == inserted.revision
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()
