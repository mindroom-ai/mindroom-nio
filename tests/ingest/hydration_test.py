"""Hydration-only result handling contract tests."""

import multiprocessing
import os
import sqlite3
from contextlib import contextmanager
from dataclasses import FrozenInstanceError, fields, is_dataclass, replace
from pathlib import Path
from uuid import UUID, uuid4

import pytest

import nio.store._sync_journal_rows as journal_rows_module
from nio.event_provenance import TimelineEventProvenance
from nio.exceptions import LocalProtocolError
from nio.ingest._json import canonical_json
from nio.ingest.config import ClassicSourceConfig
from nio.ingest.errors import JournalConflictError, JournalIntegrityError
from nio.ingest.model import EventRecord, RecordKind, RecordOrigin, TransportKind
from nio.ingest.reducer import (
    HydrationIntent,
    MembershipBaseline,
    RecoveryGap,
    RoomContinuity,
)
from nio.store._sync_journal_rows import (
    _canonical_internal,
    _canonical_room_aggregate_plaintext,
    _room_aggregate_value_from_plaintext,
)
from nio.store._sync_journal import SqliteIngestionJournal
from nio.store._sync_journal_plan import _canonical_work_plaintext
from nio.store._sync_journal_values import RoomAggregateValue

OWN_USER = "@owner:example.org"
ROOM_ID = "!room:example.org"


def _pending():
    from nio.ingest.hydration import PendingHydration

    hydration_id = uuid4()
    origin = RecordOrigin(TransportKind.CLASSIC, 1, 2, 3)
    continuity = RoomContinuity(ROOM_ID, 0, "join", None, None, hydration_id)
    return PendingHydration(continuity, HydrationIntent(hydration_id, origin))


def _member_event(event_id: str = "$member") -> dict[str, object]:
    return {
        "content": {"membership": "join"},
        "event_id": event_id,
        "state_key": OWN_USER,
        "type": "m.room.member",
    }


def test_private_hydration_values_are_frozen_slotted_and_deeply_revalidated() -> None:
    from nio.ingest.hydration import (
        HydrationResult,
        PendingHydration,
        normalize_hydration_response,
        revalidated_hydration_result,
    )

    pending = _pending()
    assert is_dataclass(PendingHydration) and [
        field.name for field in fields(PendingHydration)
    ] == ["continuity", "intent"]
    assert is_dataclass(HydrationResult) and [
        field.name for field in fields(HydrationResult)
    ] == ["pending", "response_body"]
    assert not hasattr(pending, "__dict__")
    with pytest.raises(FrozenInstanceError):
        pending.intent = pending.intent  # type: ignore[misc]
    result = normalize_hydration_response(
        pending,
        own_user_id=OWN_USER,
        response_body=b'[ { "type":"m.room.member", "state_key":"@owner:example.org", "content":{"membership":"join"}, "event_id":"$member" } ]',
    )
    assert result.response_body == canonical_json([_member_event()])
    assert revalidated_hydration_result(result, own_user_id=OWN_USER) == (
        result,
        "$member",
    )


@pytest.mark.parametrize(
    "body",
    (
        b"{}",
        b"[1]",
        b'[{"type":"x","type":"y","state_key":"","content":{},"event_id":"$x"}]',
        b'[{"type":"x","state_key":"","content":{"n":1.5},"event_id":"$x"}]',
        b'[{"type":"x","state_key":"","content":{"n":9007199254740992},"event_id":"$x"}]',
        canonical_json([{"type": "x", "state_key": "", "content": {}}]),
        canonical_json([_member_event("$") | {"content": {"membership": "leave"}}]),
        canonical_json([_member_event(), _member_event("$duplicate")]),
        canonical_json(
            [
                _member_event(),
                {
                    "type": "m.room.encrypted",
                    "state_key": "",
                    "content": {},
                    "event_id": "$e",
                },
            ]
        ),
        b"\xff",
    ),
)
def test_normalizer_rejects_noncanonical_or_untrusted_state(body: bytes) -> None:
    from nio.ingest.hydration import normalize_hydration_response

    with pytest.raises((TypeError, ValueError)):
        normalize_hydration_response(
            _pending(), own_user_id=OWN_USER, response_body=body
        )


def test_pending_identity_requires_exact_join_barrier_and_origin() -> None:
    from nio.ingest.hydration import PendingHydration

    pending = _pending()
    for continuity in (
        RoomContinuity(ROOM_ID, 0, "leave", None, None, pending.intent.hydration_id),
        RoomContinuity(ROOM_ID, 0, "join", MembershipBaseline("$x", None), None, None),
        RoomContinuity(ROOM_ID, 0, "join", None, None, uuid4()),
    ):
        with pytest.raises((TypeError, ValueError)):
            PendingHydration(continuity, pending.intent)
    with pytest.raises(ValueError):
        PendingHydration(
            pending.continuity,
            HydrationIntent(
                pending.intent.hydration_id,
                RecordOrigin(TransportKind.CLASSIC, -1, 2, 3),
            ),
        )


def test_aggregate_codec_round_trips_only_successful_membership_baseline() -> None:
    pending = _pending()
    baseline = MembershipBaseline("$member", None)
    value = RoomAggregateValue(
        RoomContinuity(ROOM_ID, 0, "join", baseline, None, None), 7, 9, None
    )
    plaintext = _canonical_room_aggregate_plaintext(value)
    assert _room_aggregate_value_from_plaintext(ROOM_ID, 9, None, plaintext) == value

    anchored = replace(
        value,
        continuity=replace(
            value.continuity,
            last_timeline_event_id="$last-seen",
        ),
    )
    anchored_plaintext = _canonical_room_aggregate_plaintext(anchored)
    assert (
        _room_aggregate_value_from_plaintext(
            ROOM_ID,
            9,
            None,
            anchored_plaintext,
        )
        == anchored
    )

    gap = RecoveryGap(uuid4(), ROOM_ID, 0, pending.intent.origin, "a", "b")
    with pytest.raises(ValueError):
        _canonical_room_aggregate_plaintext(
            RoomAggregateValue(
                RoomContinuity(ROOM_ID, 0, "join", None, gap, None), 7, 9, None
            )
        )
    with pytest.raises(ValueError):
        _canonical_room_aggregate_plaintext(
            RoomAggregateValue(
                RoomContinuity(
                    ROOM_ID,
                    0,
                    "join",
                    baseline,
                    None,
                    pending.intent.hydration_id,
                ),
                7,
                9,
                pending.intent,
            )
        )


def _open_journal(
    path: Path, statements: list[str] | None = None, account_id: str = OWN_USER
) -> SqliteIngestionJournal:
    return SqliteIngestionJournal.open(
        path,
        account_id=account_id,
        device_id="DEVICE",
        consumer_generation=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        source=ClassicSourceConfig(30_000, b"{}"),
        statement_observer=statements.append if statements is not None else None,
    )


def _seed_pending(
    journal: SqliteIngestionJournal, room_id: str = ROOM_ID, next_sequence: int = 0
):
    owner = journal.load_owner()
    hydration_id = uuid4()
    intent = HydrationIntent(hydration_id, RecordOrigin(TransportKind.CLASSIC, 1, 2, 3))
    value = RoomAggregateValue(
        RoomContinuity(room_id, 0, "join", None, None, hydration_id),
        next_sequence,
        owner.revision + 1,
        intent,
    )
    plaintext = _canonical_room_aggregate_plaintext(value)
    payload, digest = journal._payload(
        owner,
        "NioIngestRoomAggregate",
        plaintext,
        header=_canonical_internal([room_id, value.updated_revision, "hydration"]),
    )
    with journal._owner.journal_write():
        journal._execute(
            "UPDATE NioIngestMeta SET revision = ? WHERE account_id = ?",
            (value.updated_revision, journal.account_id),
        )
        journal._execute(
            "INSERT INTO NioIngestRoomAggregate VALUES (?, ?, ?, ?, ?, ?)",
            (
                journal.account_id,
                room_id,
                value.updated_revision,
                "hydration",
                payload,
                digest,
            ),
        )
    from nio.ingest.hydration import PendingHydration

    return PendingHydration(value.continuity, intent)


def _seed_held(journal: SqliteIngestionJournal, record_id: str, sequence: int) -> None:
    owner = journal.load_owner()
    record = EventRecord(
        record_id,
        RecordKind.TIMELINE,
        RecordOrigin(TransportKind.CLASSIC, 1, 2, sequence),
        ROOM_ID,
        0,
        sequence,
        None,
        TimelineEventProvenance.LIVE,
        canonical_json({"content": {"body": str(sequence)}, "type": "m.room.message"}),
        None,
    )
    clear = (
        record_id,
        "event",
        "held",
        str(uuid4()),
        ROOM_ID,
        0,
        sequence,
        None,
        None,
        owner.revision,
    )
    payload, digest = journal._payload(
        owner,
        "NioIngestWork",
        _canonical_work_plaintext("event", record),
        header=_canonical_internal(clear),
    )
    with journal._owner.journal_write():
        journal._execute(
            "INSERT INTO NioIngestWork VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (journal.account_id, *clear, payload, digest),
        )


def _grow_pending(
    journal: SqliteIngestionJournal,
    pending: object,
    next_sequence: int,
    revision: int | None = None,
) -> None:
    owner = journal.load_owner()
    revision = owner.revision + 1 if revision is None else revision
    value = RoomAggregateValue(
        pending.continuity,  # type: ignore[attr-defined]
        next_sequence,
        revision,
        pending.intent,  # type: ignore[attr-defined]
    )
    plaintext = _canonical_room_aggregate_plaintext(value)
    payload, digest = journal._payload(
        owner,
        "NioIngestRoomAggregate",
        plaintext,
        header=_canonical_internal([ROOM_ID, value.updated_revision, "hydration"]),
    )
    with journal._owner.journal_write():
        journal._execute(
            "UPDATE NioIngestMeta SET revision = ? WHERE account_id = ?",
            (value.updated_revision, OWN_USER),
        )
        journal._execute(
            "UPDATE NioIngestRoomAggregate SET updated_revision = ?, payload = ?, "
            "payload_sha256 = ? WHERE account_id = ? AND room_id = ?",
            (value.updated_revision, payload, digest, journal.account_id, ROOM_ID),
        )


def _replace_aggregate(
    journal: SqliteIngestionJournal, value: RoomAggregateValue
) -> None:
    owner = journal.load_owner()
    room_id = value.continuity.room_id
    intent_kind = "hydration" if value.pending_hydration is not None else None
    payload, digest = journal._payload(
        owner,
        "NioIngestRoomAggregate",
        _canonical_room_aggregate_plaintext(value),
        header=_canonical_internal([room_id, value.updated_revision, intent_kind]),
    )
    with journal._owner.journal_write():
        journal._execute("DELETE FROM NioIngestRoomAggregate")
        journal._execute(
            "INSERT INTO NioIngestRoomAggregate VALUES (?, ?, ?, ?, ?, ?)",
            (
                journal.account_id,
                room_id,
                value.updated_revision,
                intent_kind,
                payload,
                digest,
            ),
        )


def _raw_hydration_graph(journal: SqliteIngestionJournal) -> tuple[object, ...]:
    connection = sqlite3.connect(journal.database_path)
    try:
        return tuple(
            tuple(
                connection.execute(f"SELECT * FROM {table} ORDER BY rowid").fetchall()
            )
            for table in ("NioIngestMeta", "NioIngestRoomAggregate", "NioIngestWork")
        )
    finally:
        connection.close()


def test_aggregate_decode_cache_requires_identical_authenticated_row(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = _open_journal(tmp_path / "aggregate-cache.db")
    closed = False
    try:
        _seed_pending(journal)
        owner = journal.load_owner()
        real_decode = journal_rows_module._room_aggregate_value_from_plaintext
        decode_calls = 0

        def counted_decode(*args: object) -> RoomAggregateValue:
            nonlocal decode_calls
            decode_calls += 1
            return real_decode(*args)  # type: ignore[arg-type]

        monkeypatch.setattr(
            journal_rows_module,
            "_room_aggregate_value_from_plaintext",
            counted_decode,
        )

        loaded = journal._load_room_aggregate(owner, ROOM_ID)
        assert loaded is not None
        assert journal._load_room_aggregate(owner, ROOM_ID) == loaded
        assert decode_calls == 1

        changed = replace(loaded[1], next_room_sequence=1)
        changed_plaintext = _canonical_room_aggregate_plaintext(changed)
        changed_payload, changed_digest = journal._payload(
            owner,
            "NioIngestRoomAggregate",
            changed_plaintext,
            header=_canonical_internal(
                [ROOM_ID, changed.updated_revision, "hydration"]
            ),
        )
        with journal._owner.journal_write():
            journal._execute(
                "UPDATE NioIngestRoomAggregate SET payload = ?, payload_sha256 = ? "
                "WHERE account_id = ? AND room_id = ?",
                (changed_payload, changed_digest, journal.account_id, ROOM_ID),
            )
        changed_loaded = journal._load_room_aggregate(owner, ROOM_ID)
        assert changed_loaded is not None and changed_loaded[1] == changed
        assert decode_calls == 2

        with journal._owner.journal_write():
            journal._execute(
                "UPDATE NioIngestRoomAggregate SET payload = ? "
                "WHERE account_id = ? AND room_id = ?",
                (changed_payload + b" ", journal.account_id, ROOM_ID),
            )
        with pytest.raises(JournalIntegrityError, match="invalid room Aggregate row"):
            journal._load_room_aggregate(owner, ROOM_ID)
        assert decode_calls == 2

        with journal._owner.journal_write():
            journal._execute(
                "UPDATE NioIngestRoomAggregate SET payload = ? "
                "WHERE account_id = ? AND room_id = ?",
                (changed_payload, journal.account_id, ROOM_ID),
            )
        with pytest.raises(JournalIntegrityError, match="invalid room Aggregate row"):
            journal._load_room_aggregate(replace(owner, revision=0), ROOM_ID)
        with pytest.raises(JournalIntegrityError, match="invalid room Aggregate row"):
            journal._load_room_aggregate(replace(owner, stream_id=uuid4()), ROOM_ID)
        assert (
            journal._load_room_aggregate(
                replace(owner, account_id="@other:example.org"), ROOM_ID
            )
            == changed_loaded
        )
        assert decode_calls == 3

        other_room = "!other:example.org"
        other = replace(
            changed,
            continuity=replace(changed.continuity, room_id=other_room),
        )
        other_payload, other_digest = journal._payload(
            owner,
            "NioIngestRoomAggregate",
            _canonical_room_aggregate_plaintext(other),
            header=_canonical_internal(
                [other_room, other.updated_revision, "hydration"]
            ),
        )
        with journal._owner.journal_write():
            journal._execute(
                "INSERT INTO NioIngestRoomAggregate VALUES (?, ?, ?, ?, ?, ?)",
                (
                    journal.account_id,
                    other_room,
                    other.updated_revision,
                    "hydration",
                    other_payload,
                    other_digest,
                ),
            )
        assert journal._load_room_aggregate(owner, other_room) is not None
        assert journal._load_room_aggregate(owner, ROOM_ID) == changed_loaded
        assert decode_calls == 5

        journal.close()
        closed = True
        assert journal._room_aggregate_cache is None
    finally:
        if not closed:
            journal.close()


def test_hydration_settlement_reuses_identical_aggregate_decode(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from nio.ingest.hydration import normalize_hydration_response

    journal = _open_journal(tmp_path / "aggregate-settlement-cache.db")
    try:
        pending = _seed_pending(journal)
        result = normalize_hydration_response(
            pending,
            own_user_id=OWN_USER,
            response_body=canonical_json([_member_event()]),
        )
        real_decode = journal_rows_module._room_aggregate_value_from_plaintext
        decode_calls = 0

        def counted_decode(*args: object) -> RoomAggregateValue:
            nonlocal decode_calls
            decode_calls += 1
            return real_decode(*args)  # type: ignore[arg-type]

        monkeypatch.setattr(
            journal_rows_module,
            "_room_aggregate_value_from_plaintext",
            counted_decode,
        )

        assert journal.apply_hydration_result(result=result) is not None
        assert decode_calls == 1
    finally:
        journal.close()


def _capacity_work_row(
    journal: SqliteIngestionJournal,
    *,
    index: int,
    held_ordinal: int | None,
    padding: int,
    request_id: int = 1,
) -> tuple[tuple[object, ...], int]:
    owner = journal.load_owner()
    held = held_ordinal is not None
    record = EventRecord(
        str(UUID(int=index + 1)),
        RecordKind.TIMELINE if held else RecordKind.GLOBAL_ACCOUNT_DATA,
        RecordOrigin(TransportKind.CLASSIC, 1, request_id, index),
        ROOM_ID if held else None,
        0 if held else None,
        held_ordinal,
        None,
        TimelineEventProvenance.LIVE if held else None,
        b"x" * padding,
        None,
    )
    clear = (
        record.record_id,
        "event",
        "held" if held else "ready",
        str(UUID(int=1000 + index)),
        record.room_id,
        record.membership_epoch,
        record.room_sequence,
        None if held else owner.revision,
        None if held else index,
        owner.revision,
    )
    payload, digest = journal._payload(
        owner,
        "NioIngestWork",
        _canonical_work_plaintext("event", record),
        header=_canonical_internal(clear),
    )
    final_clear = (
        *clear[:2],
        "ready",
        *clear[3:7],
        owner.revision + 1 if held else clear[7],
        held_ordinal if held else clear[8],
        clear[9],
    )
    final_payload, _ = journal._payload(
        owner,
        "NioIngestWork",
        _canonical_work_plaintext("event", record),
        header=_canonical_internal(final_clear),
    )
    return (journal.account_id, *clear, payload, digest), len(final_payload)


def _kill_hydration_apply(path: str, boundary: str) -> None:
    from nio.ingest.hydration import normalize_hydration_response

    journal = _open_journal(Path(path))
    pending = journal.load_pending_hydrations(limit=2)[0]
    result = normalize_hydration_response(
        pending, own_user_id=OWN_USER, response_body=canonical_json([_member_event()])
    )

    def kill(label: str) -> None:
        if label == boundary:
            os._exit(86)

    journal.set_transition_statement_hook(kill)
    journal.apply_hydration_result(result=result)


def test_pending_discovery_is_bounded_ordered_and_canonically_decoded(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    journal = _open_journal(tmp_path / "discovery.db", statements)
    try:
        second = _seed_pending(journal, "!z:example.org")
        first = _seed_pending(journal, "!a:example.org")
        statements.clear()
        assert journal.load_pending_hydrations(limit=2) == (first, second)
        selects = [
            " ".join(item.split())
            for item in statements
            if "intent_kind = 'hydration'" in item
        ]
        assert selects == [
            "SELECT account_id, room_id, updated_revision, intent_kind, payload, payload_sha256 FROM NioIngestRoomAggregate WHERE account_id = '@owner:example.org' AND intent_kind = 'hydration' ORDER BY room_id LIMIT 2"
        ]
        for invalid in (False, 0, 3):
            with pytest.raises((TypeError, ValueError)):
                journal.load_pending_hydrations(limit=invalid)  # type: ignore[arg-type]
    finally:
        journal.close()


@pytest.mark.parametrize(
    "mismatch", ("absent", "id", "room", "epoch", "membership", "origin")
)
def test_every_stale_hydration_identity_returns_before_writer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mismatch: str
) -> None:
    from nio.ingest.hydration import normalize_hydration_response

    journal = _open_journal(tmp_path / f"stale-{mismatch}.db")
    try:
        pending = _seed_pending(journal)
        result = normalize_hydration_response(
            pending,
            own_user_id=OWN_USER,
            response_body=canonical_json([_member_event()]),
        )
        loaded = journal._load_room_aggregate(journal.load_owner(), ROOM_ID)
        assert loaded is not None
        value = loaded[1]
        if mismatch == "absent":
            with journal._owner.journal_write():
                journal._execute("DELETE FROM NioIngestRoomAggregate")
        else:
            continuity = value.continuity
            intent = pending.intent
            if mismatch == "id":
                hydration_id = uuid4()
                continuity = replace(continuity, hydration_id=hydration_id)
                intent = replace(intent, hydration_id=hydration_id)
            elif mismatch == "room":
                continuity = replace(continuity, room_id="!other:example.org")
            elif mismatch == "epoch":
                continuity = replace(continuity, membership_epoch=1)
            elif mismatch == "membership":
                continuity = replace(continuity, membership="leave")
            else:
                intent = replace(
                    intent,
                    origin=RecordOrigin(TransportKind.CLASSIC, 1, 2, 4),
                )
            _replace_aggregate(
                journal, replace(value, continuity=continuity, pending_hydration=intent)
            )
        before = _raw_hydration_graph(journal)

        def reject_writer():
            raise AssertionError("stale result entered journal writer")

        monkeypatch.setattr(journal._owner, "journal_write", reject_writer)
        assert journal.apply_hydration_result(result=result) is None
        assert _raw_hydration_graph(journal) == before
    finally:
        journal.close()


def test_hydration_atomically_installs_baseline_and_releases_held_in_sequence_order(
    tmp_path: Path,
) -> None:
    from nio.ingest.hydration import normalize_hydration_response

    journal = _open_journal(tmp_path / "apply.db")
    try:
        pending = _seed_pending(journal, next_sequence=1)
        high_id = "ffffffff-ffff-ffff-ffff-ffffffffffff"
        low_id = "00000000-0000-0000-0000-000000000001"
        _seed_held(journal, high_id, 0)
        result = normalize_hydration_response(
            pending,
            own_user_id=OWN_USER,
            response_body=canonical_json([_member_event()]),
        )
        _grow_pending(journal, pending, 2)
        _seed_held(journal, low_id, 1)
        before = journal.load_owner()

        committed = journal.apply_hydration_result(result=result)

        assert committed is not None and committed.revision == before.revision + 1
        owner = journal.load_owner()
        aggregate = journal._load_room_aggregate(owner, ROOM_ID)
        assert aggregate is not None
        assert aggregate[1] == RoomAggregateValue(
            RoomContinuity(
                ROOM_ID, 0, "join", MembershipBaseline("$member", None), None, None
            ),
            2,
            committed.revision,
            None,
        )
        with journal._owner.read():
            rows = journal._execute(
                "SELECT work_id, status, ready_revision, ready_ordinal, created_revision "
                "FROM NioIngestWork ORDER BY ready_ordinal"
            ).fetchall()
        assert [tuple(row) for row in rows] == [
            (high_id, "ready", committed.revision, 0, before.revision - 1),
            (low_id, "ready", committed.revision, 1, before.revision),
        ]
        assert journal.apply_hydration_result(result=result) is None
        assert journal.load_owner().revision == committed.revision
    finally:
        journal.close()


def test_hydration_release_hands_real_ready_fifo_to_claim_and_ack(
    tmp_path: Path,
) -> None:
    from nio.ingest.hydration import normalize_hydration_response

    journal = _open_journal(tmp_path / "delivery-handoff.db")
    try:
        pending = _seed_pending(journal, next_sequence=1)
        first_id = "00000000-0000-0000-0000-000000000101"
        second_id = "00000000-0000-0000-0000-000000000102"
        _seed_held(journal, first_id, 0)
        result = normalize_hydration_response(
            pending,
            own_user_id=OWN_USER,
            response_body=canonical_json([_member_event()]),
        )
        _grow_pending(journal, pending, 2)
        _seed_held(journal, second_id, 1)
        committed = journal.apply_hydration_result(result=result)
        assert committed is not None

        first = journal.next_batch()
        assert first is not None
        assert first.created_revision == committed.revision
        assert first.ref.sequence == 0
        assert first.records[0].record_id == first_id
        assert journal.load_owner().revision == committed.revision + 1
        journal.acknowledge_batch(first.ref)
        assert journal.load_owner().revision == committed.revision + 2

        second = journal.next_batch()
        assert second is not None
        assert second.created_revision == committed.revision
        assert second.ref.sequence == 1
        assert second.records[0].record_id == second_id
        journal.acknowledge_batch(second.ref)
        assert journal.next_batch() is None
    finally:
        journal.close()


@pytest.mark.parametrize(
    "boundary",
    ("meta_revision_epoch_cas", "aggregate_update", "work_release", "before_commit"),
)
def test_hydration_transition_exception_rolls_back_and_retry_commits(
    tmp_path: Path, boundary: str
) -> None:
    from nio.ingest.hydration import normalize_hydration_response

    journal = _open_journal(tmp_path / f"rollback-{boundary}.db")
    try:
        pending = _seed_pending(journal, next_sequence=1)
        _seed_held(journal, "00000000-0000-0000-0000-000000000001", 0)
        result = normalize_hydration_response(
            pending,
            own_user_id=OWN_USER,
            response_body=canonical_json([_member_event()]),
        )
        before = journal.load_owner()

        def fail(label: str) -> None:
            if label == boundary:
                raise RuntimeError(boundary)

        journal.set_transition_statement_hook(fail)
        with pytest.raises(RuntimeError, match=boundary):
            journal.apply_hydration_result(result=result)
        assert journal.load_owner() == before
        assert len(journal.load_pending_hydrations(limit=2)) == 1
        with journal._owner.read():
            assert (
                journal._execute("SELECT status FROM NioIngestWork").fetchone()[0]
                == "held"
            )
        journal.set_transition_statement_hook(None)
        assert journal.apply_hydration_result(result=result) is not None
    finally:
        journal.close()


@pytest.mark.parametrize("delta", (0, 1))
def test_hydration_enforces_final_ready_record_envelope_limit(
    tmp_path: Path, delta: int
) -> None:
    from nio.ingest.hydration import normalize_hydration_response

    journal = None
    try:
        target = 1024 * 1024 + delta
        seen = []
        for suffix, digits in ((s, d) for d in range(1, 19) for s in range(4)):
            account_id = OWN_USER + "x" * suffix
            candidate = _open_journal(
                tmp_path / f"record-cap-{delta}-{suffix}-{digits}.db",
                account_id=account_id,
            )
            pending = _seed_pending(candidate, next_sequence=1)
            _grow_pending(candidate, pending, 1, 10**digits)
            owner = candidate.load_owner()

            def envelope(padding: int, ready: bool):
                record = EventRecord(
                    "00000000-0000-0000-0000-000000000001",
                    RecordKind.TIMELINE,
                    RecordOrigin(TransportKind.CLASSIC, 1, 2, 0),
                    ROOM_ID,
                    0,
                    0,
                    None,
                    TimelineEventProvenance.LIVE,
                    b"x" * padding,
                    None,
                )
                clear = (
                    record.record_id,
                    "event",
                    "ready" if ready else "held",
                    "00000000-0000-0000-0000-000000000002",
                    ROOM_ID,
                    0,
                    0,
                    owner.revision + 1 if ready else None,
                    0 if ready else None,
                    1,
                )
                payload, digest = candidate._payload(
                    owner,
                    "NioIngestWork",
                    _canonical_work_plaintext("event", record),
                    header=_canonical_internal(clear),
                )
                return clear, payload, digest

            low, high = 0, target
            while low <= high:
                middle = (low + high) // 2
                if len(envelope(middle, True)[1]) <= target:
                    low = middle + 1
                else:
                    high = middle - 1
            clear, payload, digest = envelope(high, False)
            ready_size = len(envelope(high, True)[1])
            seen.append((suffix, digits, ready_size, len(payload)))
            if ready_size == target and len(payload) <= 1024 * 1024:
                journal = candidate
                break
            candidate.close()
        assert journal is not None, seen
        with journal._owner.journal_write():
            journal._execute(
                "INSERT INTO NioIngestWork VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (journal.account_id, *clear, payload, digest),
            )
        result = normalize_hydration_response(
            pending,
            own_user_id=journal.account_id,
            response_body=canonical_json(
                [_member_event() | {"state_key": journal.account_id}]
            ),
        )
        before = journal.load_owner()
        if delta:
            with pytest.raises(ValueError, match="record capacity"):
                journal.apply_hydration_result(result=result)
            assert journal.load_owner() == before
        else:
            assert journal.apply_hydration_result(result=result) is not None
            path, account_id = journal.database_path, journal.account_id
            journal.close()
            journal = _open_journal(path, account_id=account_id)
            owner = journal.load_owner()
            assert journal.load_pending_hydrations(limit=2) == ()
            assert journal._load_task3_work_inventory(owner).work[0].status == "ready"
    finally:
        if journal is not None:
            journal.close()


@pytest.mark.parametrize("delta", (0, 1))
def test_hydration_applies_real_final_total_boundary(
    tmp_path: Path, delta: int
) -> None:
    from nio.ingest.hydration import normalize_hydration_response

    journal = _open_journal(tmp_path / f"total-cap-{delta}.db")
    try:
        pending = _seed_pending(journal, next_sequence=32)
        _grow_pending(journal, pending, 32, 10**18)
        target = 64 * 1024 * 1024 + delta
        row_target = target // 65
        rows: list[tuple[object, ...]] = []
        final_total = 0
        for index in range(64):
            held_ordinal = index - 33 if index >= 33 else None
            _, empty_size = _capacity_work_row(
                journal, index=index, held_ordinal=held_ordinal, padding=0
            )
            padding = max(0, (row_target - empty_size) // 4 * 3)
            row, final_size = _capacity_work_row(
                journal,
                index=index,
                held_ordinal=held_ordinal,
                padding=padding,
            )
            assert final_size <= row_target < final_size + 4
            rows.append(row)
            final_total += final_size
        remaining = target - final_total
        for request_id in (1, 10, 100, 1000):
            _, empty_size = _capacity_work_row(
                journal,
                index=64,
                held_ordinal=31,
                padding=0,
                request_id=request_id,
            )
            if remaining >= empty_size and (remaining - empty_size) % 4 == 0:
                row, final_size = _capacity_work_row(
                    journal,
                    index=64,
                    held_ordinal=31,
                    padding=(remaining - empty_size) // 4 * 3,
                    request_id=request_id,
                )
                if final_size == remaining:
                    rows.append(row)
                    final_total += final_size
                    break
        assert len(rows) == 65 and final_total == target
        assert max(len(row[11]) for row in rows) <= 1024 * 1024
        assert sum(len(row[11]) for row in rows[33:]) <= 32 * 1024 * 1024
        with journal._owner.journal_write():
            for row in rows:
                journal._execute(
                    "INSERT INTO NioIngestWork VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    row,
                )
        result = normalize_hydration_response(
            pending,
            own_user_id=OWN_USER,
            response_body=canonical_json([_member_event()]),
        )
        before = _raw_hydration_graph(journal)
        if delta:
            with pytest.raises(ValueError, match="total capacity"):
                journal.apply_hydration_result(result=result)
            assert _raw_hydration_graph(journal) == before
        else:
            assert journal.apply_hydration_result(result=result) is not None
            path = journal.database_path
            journal.close()
            journal = _open_journal(path)
            owner = journal.load_owner()
            inventory = journal._load_task3_work_inventory(owner)
            assert sum(len(row[11]) for row in inventory.storage_rows) == target
            assert all(item.status == "ready" for item in inventory.work)
            assert journal.load_pending_hydrations(limit=2) == ()
    finally:
        journal.close()


@pytest.mark.parametrize(
    "race", ("revision", "epoch", "aggregate", "work_insert", "work_mutation")
)
def test_hydration_writer_rechecks_every_ownership_snapshot(
    tmp_path: Path, race: str, monkeypatch: pytest.MonkeyPatch
) -> None:
    from nio.ingest.hydration import normalize_hydration_response

    journal = _open_journal(tmp_path / f"race-{race}.db")
    try:
        pending = _seed_pending(journal, next_sequence=1)
        _seed_held(journal, "00000000-0000-0000-0000-000000000001", 0)
        result = normalize_hydration_response(
            pending,
            own_user_id=OWN_USER,
            response_body=canonical_json([_member_event()]),
        )
        owner = journal.load_owner()
        aggregate = journal._load_room_aggregate(owner, ROOM_ID)
        assert aggregate is not None
        aggregate_replacement = replace(aggregate[1], next_room_sequence=2)
        aggregate_payload, aggregate_digest = journal._payload(
            owner,
            "NioIngestRoomAggregate",
            _canonical_room_aggregate_plaintext(aggregate_replacement),
            header=_canonical_internal(
                [ROOM_ID, aggregate_replacement.updated_revision, "hydration"]
            ),
        )
        inventory = journal._load_task3_work_inventory(owner)
        work_row = inventory.storage_rows[0]
        changed_record = replace(
            inventory.work[0].value,
            source_json=b'{"content":{"raced":true},"type":"m.room.message"}',
        )
        changed_payload, changed_digest = journal._payload(
            owner,
            "NioIngestWork",
            _canonical_work_plaintext("event", changed_record),
            header=_canonical_internal(work_row[1:11]),
        )
        inserted_row, _ = _capacity_work_row(
            journal, index=1, held_ordinal=1, padding=0
        )
        real_transaction = journal._transaction
        raced = False
        raced_graph = None

        @contextmanager
        def racing_transaction():
            nonlocal raced, raced_graph
            if not raced:
                raced = True
                connection = sqlite3.connect(journal.database_path)
                try:
                    if race == "revision":
                        connection.execute(
                            "UPDATE NioIngestMeta SET revision = revision + 1"
                        )
                    elif race == "epoch":
                        connection.execute(
                            "UPDATE NioIngestMeta SET writer_epoch = ?", (str(uuid4()),)
                        )
                    elif race == "aggregate":
                        connection.execute(
                            "UPDATE NioIngestRoomAggregate SET payload = ?, payload_sha256 = ?",
                            (aggregate_payload, aggregate_digest),
                        )
                    elif race == "work_insert":
                        connection.execute(
                            "INSERT INTO NioIngestWork VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                            inserted_row,
                        )
                    else:
                        connection.execute(
                            "UPDATE NioIngestWork SET payload = ?, payload_sha256 = ? WHERE work_id = ?",
                            (
                                changed_payload,
                                changed_digest,
                                work_row[1],
                            ),
                        )
                    connection.commit()
                finally:
                    connection.close()
                raced_graph = _raw_hydration_graph(journal)
            with real_transaction():
                yield

        monkeypatch.setattr(journal, "_transaction", racing_transaction)
        error = {
            "revision": JournalConflictError,
            "epoch": LocalProtocolError,
        }.get(race, JournalIntegrityError)
        with pytest.raises(error):
            journal.apply_hydration_result(result=result)
        assert raced_graph is not None
        assert _raw_hydration_graph(journal) == raced_graph
    finally:
        journal.close()


@pytest.mark.parametrize(
    ("boundary", "committed"),
    (
        ("meta_revision_epoch_cas", False),
        ("aggregate_update", False),
        ("work_release", False),
        ("before_commit", False),
        ("commit", True),
    ),
)
def test_hydration_spawned_kill_reopens_only_old_or_full_new(
    tmp_path: Path, boundary: str, committed: bool
) -> None:
    path = tmp_path / f"kill-{boundary}.db"
    journal = _open_journal(path)
    pending = _seed_pending(journal, next_sequence=1)
    _seed_held(journal, "00000000-0000-0000-0000-000000000001", 0)
    journal.close()
    process = multiprocessing.get_context("spawn").Process(
        target=_kill_hydration_apply, args=(str(path), boundary)
    )
    process.start()
    process.join(20)
    assert process.exitcode == 86
    reopened = _open_journal(path)
    try:
        owner = reopened.load_owner()
        work = reopened._load_task3_work_inventory(owner).work
        assert [item.status for item in work] == (["ready"] if committed else ["held"])
        assert bool(reopened.load_pending_hydrations(limit=2)) is not committed
        if not committed:
            from nio.ingest.hydration import normalize_hydration_response

            result = normalize_hydration_response(
                pending,
                own_user_id=OWN_USER,
                response_body=canonical_json([_member_event()]),
            )
            assert reopened.apply_hydration_result(result=result) is not None
    finally:
        reopened.close()
