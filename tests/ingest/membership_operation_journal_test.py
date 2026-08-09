"""Durable membership-operation journal state-machine tests."""

import hashlib
import json
import sqlite3
from dataclasses import fields, replace
from inspect import Parameter, signature
from pathlib import Path
from uuid import UUID, uuid5

import pytest

from nio.exceptions import LocalProtocolError
from nio.ingest import (
    ConsumerBinding,
    ConsumerBootstrap,
    RoomHydrationStatus,
    TransportKind,
)
from nio.ingest.config import ClassicSourceConfig
from nio.ingest.effects import (
    MembershipAction,
    MembershipDeliveryState,
    MembershipOperationRef,
    MembershipOperationResolution,
    MembershipOperationResolutionOutcome,
    MembershipOperationStatus,
    MembershipRequest,
    PersistedNetworkEffect,
    RoomHydrationRequest,
)
from nio.ingest.errors import JournalConflictError, JournalIntegrityError
from nio.ingest.state import JournalTransition, LaneStatus, RoomLane, RoomState
from nio.store._sync_journal_port import IngestionJournal
from nio.store.sync_journal import open_ingestion_store

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "DEVICE"
JOURNAL_GENERATION = UUID("11111111-1111-1111-1111-111111111111")
CONSUMER_GENERATION = UUID("22222222-2222-2222-2222-222222222222")
EFFECT_ID = UUID("55555555-5555-5555-5555-555555555555")
CLASSIC_SOURCE = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")


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
    effect_id: UUID = EFFECT_ID,
    room_id: str = "!membership",
) -> PersistedNetworkEffect:
    return PersistedNetworkEffect(
        MembershipRequest(
            effect_id,
            stream_id,
            TransportKind.CLASSIC,
            room_id,
            0,
            MembershipAction.JOIN,
            b'{"reason":"restore"}',
            30_000,
        ),
        0,
        MembershipDeliveryState.READY,
        False,
    )


async def _open_with_membership(tmp_path: Path):
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer(bootstrap, ("!membership",))
    await bootstrap.attach_consumer(consumer)
    effect = _membership(bootstrap.stream_id)
    journal = bootstrap._journal
    owner = journal.load_owner()
    journal.commit(
        expected_revision=owner.revision,
        writer_epoch=journal.writer_epoch,
        transition=JournalTransition(network_effect_inserts=(effect,)),
    )
    return bootstrap, consumer, effect


def test_membership_operation_port_and_uncertain_index_are_exact(
    tmp_path: Path,
) -> None:
    assert {
        "claim_membership_operation",
        "uncertain_membership_operations",
        "resolve_membership_operation",
    } <= IngestionJournal.__dict__.keys()
    claim = signature(IngestionJournal.claim_membership_operation)
    assert tuple(claim.parameters) == ("self", "effect_id")
    uncertain = signature(IngestionJournal.uncertain_membership_operations)
    assert tuple(uncertain.parameters) == ("self", "limit", "after_effect_id")
    assert uncertain.parameters["after_effect_id"].kind is Parameter.KEYWORD_ONLY
    assert uncertain.parameters["after_effect_id"].default is None
    resolve = signature(IngestionJournal.resolve_membership_operation)
    assert tuple(resolve.parameters) == ("self", "ref", "resolution")

    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
    )
    bootstrap.close()
    with sqlite3.connect(tmp_path / "journal.db") as connection:
        row = connection.execute(
            "SELECT sql FROM sqlite_master WHERE type='index' "
            "AND name='NioIngestNetworkEffect_uncertain'"
        ).fetchone()
        assert row is not None
        assert "(account_id, effect_id)" in row[0]
        assert "effect_kind = 'membership'" in row[0]
        assert "membership_delivery_state = 'dispatched_unconfirmed'" in row[0]


@pytest.mark.asyncio
async def test_claim_authenticates_and_durably_dispatches_before_return(
    tmp_path: Path,
) -> None:
    bootstrap, consumer, initial = await _open_with_membership(tmp_path)
    journal = bootstrap._journal
    before = journal.load_owner()
    before_row = tuple(
        journal.connection.execute(
            "SELECT request_ciphertext, request_sha256, created_revision "
            "FROM NioIngestNetworkEffect WHERE account_id=? AND effect_id=?",
            (ACCOUNT_ID, str(EFFECT_ID)),
        ).fetchone()
    )
    try:
        request, ref = journal.claim_membership_operation(EFFECT_ID)

        assert request == initial.request
        assert ref == MembershipOperationRef(
            EFFECT_ID,
            "!membership",
            0,
            1,
            hashlib.sha256(journal._network_effect_request_payload(request)).digest(),
        )
        assert journal.load_owner().revision == before.revision + 1
        after_row = tuple(
            journal.connection.execute(
                "SELECT request_ciphertext, request_sha256, created_revision "
                "FROM NioIngestNetworkEffect WHERE account_id=? AND effect_id=?",
                (ACCOUNT_ID, str(EFFECT_ID)),
            ).fetchone()
        )
        assert after_row == before_row
        assert journal.load_network_effect(EFFECT_ID) == PersistedNetworkEffect(
            request,
            1,
            MembershipDeliveryState.DISPATCHED_UNCONFIRMED,
            False,
        )
        assert journal.list_schedulable_network_effects(256) == ()
        assert journal.uncertain_membership_operations(256) == (
            MembershipOperationStatus(
                ref,
                MembershipAction.JOIN,
                MembershipDeliveryState.DISPATCHED_UNCONFIRMED,
                False,
            ),
        )
        with pytest.raises(JournalIntegrityError, match="READY|ready"):
            journal.claim_membership_operation(EFFECT_ID)
        assert journal.load_owner().revision == before.revision + 1
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
        assert reopened._journal.uncertain_membership_operations(256)[0].ref == ref
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_retry_is_exact_idempotent_then_the_old_ref_becomes_stale(
    tmp_path: Path,
) -> None:
    bootstrap, _, _ = await _open_with_membership(tmp_path)
    journal = bootstrap._journal
    try:
        _, first_ref = journal.claim_membership_operation(EFFECT_ID)
        immutable_row = tuple(
            journal.connection.execute(
                "SELECT request_ciphertext, request_sha256, created_revision "
                "FROM NioIngestNetworkEffect WHERE account_id=? AND effect_id=?",
                (ACCOUNT_ID, str(EFFECT_ID)),
            ).fetchone()
        )
        before_retry = journal.load_owner().revision
        assert (
            journal.resolve_membership_operation(
                first_ref,
                MembershipOperationResolution.RETRY,
            )
            is MembershipOperationResolutionOutcome.READY
        )
        after_retry = journal.load_owner().revision
        assert after_retry == before_retry + 1
        assert (
            tuple(
                journal.connection.execute(
                    "SELECT request_ciphertext, request_sha256, created_revision "
                    "FROM NioIngestNetworkEffect WHERE account_id=? AND effect_id=?",
                    (ACCOUNT_ID, str(EFFECT_ID)),
                ).fetchone()
            )
            == immutable_row
        )
        ready_row = tuple(
            journal.connection.execute(
                "SELECT * FROM NioIngestNetworkEffect "
                "WHERE account_id=? AND effect_id=?",
                (ACCOUNT_ID, str(EFFECT_ID)),
            ).fetchone()
        )
        statements: list[str] = []
        journal.connection.set_trace_callback(statements.append)
        assert (
            journal.resolve_membership_operation(
                first_ref,
                MembershipOperationResolution.RETRY,
            )
            is MembershipOperationResolutionOutcome.READY
        )
        journal.connection.set_trace_callback(None)
        assert journal.load_owner().revision == after_retry
        assert (
            tuple(
                journal.connection.execute(
                    "SELECT * FROM NioIngestNetworkEffect "
                    "WHERE account_id=? AND effect_id=?",
                    (ACCOUNT_ID, str(EFFECT_ID)),
                ).fetchone()
            )
            == ready_row
        )
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )

        request, second_ref = journal.claim_membership_operation(EFFECT_ID)
        assert second_ref.attempt_ordinal == 2
        assert second_ref.request_sha256 == first_ref.request_sha256
        assert journal.load_network_effect(EFFECT_ID) == PersistedNetworkEffect(
            request,
            2,
            MembershipDeliveryState.DISPATCHED_UNCONFIRMED,
            True,
        )
        revision = journal.load_owner().revision
        with pytest.raises(JournalIntegrityError, match="attempt|stale|reference"):
            journal.resolve_membership_operation(
                first_ref,
                MembershipOperationResolution.RETRY,
            )
        assert journal.load_owner().revision == revision
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_supersede_deletes_exact_operation_and_absent_repeat_is_noop(
    tmp_path: Path,
) -> None:
    bootstrap, _, _ = await _open_with_membership(tmp_path)
    journal = bootstrap._journal
    try:
        _, ref = journal.claim_membership_operation(EFFECT_ID)
        room_rows = tuple(
            tuple(row)
            for table in ("NioIngestRoomState", "NioIngestRoomLane")
            for row in journal.connection.execute(
                f"SELECT * FROM {table} WHERE account_id=? AND room_id=?",
                (ACCOUNT_ID, "!membership"),
            )
        )
        before = journal.load_owner().revision
        assert (
            journal.resolve_membership_operation(
                ref,
                MembershipOperationResolution.SUPERSEDE,
            )
            is MembershipOperationResolutionOutcome.SUPERSEDED
        )
        assert journal.load_owner().revision == before + 1
        assert journal.load_network_effect(EFFECT_ID) is None
        assert (
            tuple(
                tuple(row)
                for table in ("NioIngestRoomState", "NioIngestRoomLane")
                for row in journal.connection.execute(
                    f"SELECT * FROM {table} WHERE account_id=? AND room_id=?",
                    (ACCOUNT_ID, "!membership"),
                )
            )
            == room_rows
        )
        after = journal.load_owner().revision
        assert (
            journal.resolve_membership_operation(
                ref,
                MembershipOperationResolution.SUPERSEDE,
            )
            is MembershipOperationResolutionOutcome.ABSENT
        )
        assert journal.load_owner().revision == after
    finally:
        bootstrap.close()


def test_uncertain_status_has_no_request_or_body_fields() -> None:
    assert tuple(field.name for field in fields(MembershipOperationStatus)) == (
        "ref",
        "action",
        "delivery_state",
        "prior_delivery_uncertain",
    )
    assert "request" not in MembershipOperationStatus.__slots__
    assert "request_body" not in MembershipOperationStatus.__slots__


@pytest.mark.asyncio
async def test_membership_operation_inputs_and_absent_outcomes_are_exact(
    tmp_path: Path,
) -> None:
    bootstrap, _, _ = await _open_with_membership(tmp_path)
    journal = bootstrap._journal
    absent_ref = MembershipOperationRef(
        UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        "!absent",
        0,
        1,
        b"a" * 32,
    )

    class ForeignRef(MembershipOperationRef):
        pass

    try:
        with pytest.raises(TypeError, match="effect_id"):
            journal.claim_membership_operation(str(EFFECT_ID))  # type: ignore[arg-type]
        for limit in (True, 0, -1, 257):
            expected = TypeError if type(limit) is bool else ValueError
            with pytest.raises(expected):
                journal.uncertain_membership_operations(limit)  # type: ignore[arg-type]
        for cursor in (str(EFFECT_ID), False):
            with pytest.raises(TypeError, match="after_effect_id"):
                journal.uncertain_membership_operations(
                    1,
                    after_effect_id=cursor,  # type: ignore[arg-type]
                )
        foreign = ForeignRef(
            absent_ref.effect_id,
            absent_ref.room_id,
            absent_ref.membership_epoch,
            absent_ref.attempt_ordinal,
            absent_ref.request_sha256,
        )
        with pytest.raises(TypeError, match="ref"):
            journal.resolve_membership_operation(
                foreign,
                MembershipOperationResolution.RETRY,
            )
        with pytest.raises(TypeError, match="resolution"):
            journal.resolve_membership_operation(
                absent_ref,
                "retry",  # type: ignore[arg-type]
            )

        revision = journal.load_owner().revision
        statements: list[str] = []
        journal.connection.set_trace_callback(statements.append)
        with pytest.raises(JournalIntegrityError, match="does not exist"):
            journal.resolve_membership_operation(
                absent_ref,
                MembershipOperationResolution.RETRY,
            )
        assert (
            journal.resolve_membership_operation(
                absent_ref,
                MembershipOperationResolution.SUPERSEDE,
            )
            is MembershipOperationResolutionOutcome.ABSENT
        )
        journal.connection.set_trace_callback(None)
        assert journal.load_owner().revision == revision
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_membership_operation_methods_are_closed_before_attachment(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        database_name="journal.db",
    )
    ref = MembershipOperationRef(EFFECT_ID, "!membership", 0, 1, b"a" * 32)
    try:
        for operation in (
            lambda: bootstrap._journal.claim_membership_operation(EFFECT_ID),
            lambda: bootstrap._journal.uncertain_membership_operations(1),
            lambda: bootstrap._journal.resolve_membership_operation(
                ref,
                MembershipOperationResolution.SUPERSEDE,
            ),
        ):
            with pytest.raises(LocalProtocolError, match="not attached"):
                operation()
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_closed_membership_mutators_refuse_before_begin(tmp_path: Path) -> None:
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
    await bootstrap.attach_consumer(_consumer(bootstrap, ("!membership",)))
    effect = _membership(bootstrap.stream_id)
    journal = bootstrap._journal
    owner = journal.load_owner()
    journal.commit(
        expected_revision=owner.revision,
        writer_epoch=journal.writer_epoch,
        transition=JournalTransition(network_effect_inserts=(effect,)),
    )
    _, ref = journal.claim_membership_operation(EFFECT_ID)
    statements.clear()
    bootstrap.close()

    for operation in (
        lambda: journal.claim_membership_operation(EFFECT_ID),
        lambda: journal.resolve_membership_operation(
            ref,
            MembershipOperationResolution.RETRY,
        ),
    ):
        with pytest.raises(LocalProtocolError, match="closed"):
            operation()
    assert "BEGIN IMMEDIATE" not in statements


@pytest.mark.asyncio
async def test_replaced_lock_membership_mutators_refuse_before_sql(
    tmp_path: Path,
) -> None:
    bootstrap, _, _ = await _open_with_membership(tmp_path)
    journal = bootstrap._journal
    _, ref = journal.claim_membership_operation(EFFECT_ID)
    lock_path = Path(f"{tmp_path / 'journal.db'}.ingest.lock")
    lock_path.unlink()
    lock_path.write_bytes(b"replacement")
    statements: list[str] = []
    journal.connection.set_trace_callback(statements.append)
    try:
        for operation in (
            lambda: journal.claim_membership_operation(EFFECT_ID),
            lambda: journal.resolve_membership_operation(
                ref,
                MembershipOperationResolution.RETRY,
            ),
        ):
            with pytest.raises(LocalProtocolError, match="lock file identity"):
                operation()
        assert statements == []
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_ready_without_prior_uncertainty_cannot_use_operator_resolution(
    tmp_path: Path,
) -> None:
    bootstrap, _, _ = await _open_with_membership(tmp_path)
    journal = bootstrap._journal
    try:
        _, ref = journal.claim_membership_operation(EFFECT_ID)
        dispatched = journal.load_network_effect(EFFECT_ID)
        assert dispatched is not None
        owner = journal.load_owner()
        ready = replace(
            dispatched,
            membership_delivery_state=MembershipDeliveryState.READY,
        )
        journal.commit(
            expected_revision=owner.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(network_effect_updates=(ready,)),
        )
        revision = journal.load_owner().revision
        statements: list[str] = []
        journal.connection.set_trace_callback(statements.append)
        for resolution in MembershipOperationResolution:
            with pytest.raises(JournalIntegrityError, match="DISPATCHED|dispatched"):
                journal.resolve_membership_operation(ref, resolution)
        journal.connection.set_trace_callback(None)
        assert journal.load_owner().revision == revision
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("field_name", "forged_value"),
    (
        ("room_id", "!other"),
        ("membership_epoch", 1),
        ("attempt_ordinal", 2),
        ("request_sha256", b"x" * 32),
    ),
)
@pytest.mark.asyncio
async def test_live_membership_reference_mismatch_rejects_before_dml(
    tmp_path: Path,
    field_name: str,
    forged_value: object,
) -> None:
    bootstrap, _, _ = await _open_with_membership(tmp_path)
    journal = bootstrap._journal
    try:
        _, ref = journal.claim_membership_operation(EFFECT_ID)
        forged = replace(ref, **{field_name: forged_value})
        revision = journal.load_owner().revision
        statements: list[str] = []
        journal.connection.set_trace_callback(statements.append)
        for resolution in MembershipOperationResolution:
            with pytest.raises(JournalIntegrityError, match="reference|stale"):
                journal.resolve_membership_operation(forged, resolution)
        journal.connection.set_trace_callback(None)
        assert journal.load_owner().revision == revision
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_changed_live_request_cannot_be_resolved_with_old_reference(
    tmp_path: Path,
) -> None:
    bootstrap, _, _ = await _open_with_membership(tmp_path)
    journal = bootstrap._journal
    try:
        request, ref = journal.claim_membership_operation(EFFECT_ID)
        changed = replace(
            request,
            action=MembershipAction.LEAVE,
            request_body=b'{"reason":"changed"}',
        )
        payload = journal._network_effect_request_payload(changed)
        ciphertext, digest = journal._codec.seal(
            "NioIngestNetworkEffect.request",
            (EFFECT_ID,),
            payload,
        )
        journal.connection.execute(
            "UPDATE NioIngestNetworkEffect SET request_ciphertext=?, request_sha256=? "
            "WHERE account_id=? AND effect_id=?",
            (ciphertext, digest, ACCOUNT_ID, str(EFFECT_ID)),
        )
        revision = journal.load_owner().revision
        statements: list[str] = []
        journal.connection.set_trace_callback(statements.append)
        with pytest.raises(JournalIntegrityError, match="reference|stale"):
            journal.resolve_membership_operation(
                ref,
                MembershipOperationResolution.SUPERSEDE,
            )
        journal.connection.set_trace_callback(None)
        assert journal.load_owner().revision == revision
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize("target_kind", ("membership", "hydration"))
@pytest.mark.asyncio
async def test_reference_effect_id_swap_to_another_live_row_never_looks_absent(
    tmp_path: Path,
    target_kind: str,
) -> None:
    rooms = ("!membership", "!target")
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer(bootstrap, rooms))
    journal = bootstrap._journal
    first = _membership(bootstrap.stream_id)
    target_id = UUID("66666666-6666-4666-8666-666666666666")
    if target_kind == "membership":
        target = _membership(
            bootstrap.stream_id,
            effect_id=target_id,
            room_id="!target",
        )
    else:
        target = PersistedNetworkEffect(
            RoomHydrationRequest(
                uuid5(bootstrap.stream_id, "hydrate:!target:0"),
                bootstrap.stream_id,
                TransportKind.CLASSIC,
                "!target",
                0,
                30_000,
            ),
            0,
            None,
            None,
        )
        target_id = target.request.effect_id
    try:
        owner = journal.load_owner()
        journal.commit(
            expected_revision=owner.revision,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(network_effect_inserts=(first, target)),
        )
        _, ref = journal.claim_membership_operation(EFFECT_ID)
        swapped = replace(ref, effect_id=target_id)
        revision = journal.load_owner().revision
        for resolution in MembershipOperationResolution:
            with pytest.raises(JournalIntegrityError):
                journal.resolve_membership_operation(swapped, resolution)
        assert journal.load_owner().revision == revision
        assert journal.load_network_effect(target_id) == target
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_current_room_epoch_drift_rejects_claim_before_dml(
    tmp_path: Path,
) -> None:
    bootstrap, _, _ = await _open_with_membership(tmp_path)
    journal = bootstrap._journal
    try:
        owner = journal.load_owner()
        journal.connection.execute(
            "DELETE FROM NioIngestRoomLane WHERE account_id=? AND room_id=?",
            (ACCOUNT_ID, "!membership"),
        )
        journal._write_room_state(
            RoomState(
                "!membership",
                1,
                0,
                RoomHydrationStatus.PENDING,
                None,
            ),
            owner.revision,
        )
        journal._write_room_lane(
            RoomLane("!membership", 1, LaneStatus.ACTIVE),
            owner.revision,
            owner.transport_kind,
        )
        revision = journal.load_owner().revision
        statements: list[str] = []
        journal.connection.set_trace_callback(statements.append)
        with pytest.raises(JournalIntegrityError, match="epoch"):
            journal.claim_membership_operation(EFFECT_ID)
        journal.connection.set_trace_callback(None)
        assert journal.load_owner().revision == revision
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize("resolution", tuple(MembershipOperationResolution))
@pytest.mark.asyncio
async def test_current_room_epoch_drift_rejects_resolution_before_dml(
    tmp_path: Path,
    resolution: MembershipOperationResolution,
) -> None:
    bootstrap, _, _ = await _open_with_membership(tmp_path)
    journal = bootstrap._journal
    try:
        _, ref = journal.claim_membership_operation(EFFECT_ID)
        owner = journal.load_owner()
        journal.connection.execute(
            "DELETE FROM NioIngestRoomLane WHERE account_id=? AND room_id=?",
            (ACCOUNT_ID, "!membership"),
        )
        journal._write_room_state(
            RoomState(
                "!membership",
                1,
                0,
                RoomHydrationStatus.PENDING,
                None,
            ),
            owner.revision,
        )
        journal._write_room_lane(
            RoomLane("!membership", 1, LaneStatus.ACTIVE),
            owner.revision,
            owner.transport_kind,
        )
        revision = journal.load_owner().revision
        statements: list[str] = []
        journal.connection.set_trace_callback(statements.append)
        with pytest.raises(JournalIntegrityError, match="epoch"):
            journal.resolve_membership_operation(ref, resolution)
        journal.connection.set_trace_callback(None)
        assert journal.load_owner().revision == revision
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_membership_mutations_fence_persisted_writer_epoch(
    tmp_path: Path,
) -> None:
    bootstrap, _, _ = await _open_with_membership(tmp_path)
    journal = bootstrap._journal
    try:
        journal.connection.execute(
            "UPDATE NioIngestMeta SET writer_epoch=? WHERE account_id=?",
            (str(UUID(int=9876)), ACCOUNT_ID),
        )
        revision = journal.load_owner().revision
        statements: list[str] = []
        journal.connection.set_trace_callback(statements.append)
        with pytest.raises(JournalConflictError, match="writer_epoch"):
            journal.claim_membership_operation(EFFECT_ID)
        journal.connection.set_trace_callback(None)
        assert journal.load_owner().revision == revision
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize("operation", ("retry", "retry_noop", "absent_supersede"))
@pytest.mark.asyncio
async def test_membership_resolution_noop_paths_still_fence_writer_epoch(
    tmp_path: Path,
    operation: str,
) -> None:
    bootstrap, _, _ = await _open_with_membership(tmp_path)
    journal = bootstrap._journal
    try:
        _, ref = journal.claim_membership_operation(EFFECT_ID)
        if operation == "retry_noop":
            journal.resolve_membership_operation(
                ref,
                MembershipOperationResolution.RETRY,
            )
        elif operation == "absent_supersede":
            journal.resolve_membership_operation(
                ref,
                MembershipOperationResolution.SUPERSEDE,
            )
        journal.connection.execute(
            "UPDATE NioIngestMeta SET writer_epoch=? WHERE account_id=?",
            (str(UUID(int=9876)), ACCOUNT_ID),
        )
        revision = journal.load_owner().revision
        statements: list[str] = []
        journal.connection.set_trace_callback(statements.append)
        resolution = (
            MembershipOperationResolution.SUPERSEDE
            if operation == "absent_supersede"
            else MembershipOperationResolution.RETRY
        )
        with pytest.raises(JournalConflictError, match="writer_epoch"):
            journal.resolve_membership_operation(ref, resolution)
        journal.connection.set_trace_callback(None)
        assert journal.load_owner().revision == revision
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_claim_rejects_absent_and_live_nonmembership_without_dml(
    tmp_path: Path,
) -> None:
    rooms = ("!hydration", "!membership")
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer(bootstrap, rooms))
    journal = bootstrap._journal
    hydration_id = uuid5(bootstrap.stream_id, "hydrate:!hydration:0")
    hydration = PersistedNetworkEffect(
        RoomHydrationRequest(
            hydration_id,
            bootstrap.stream_id,
            TransportKind.CLASSIC,
            "!hydration",
            0,
            30_000,
        ),
        0,
        None,
        None,
    )
    owner = journal.load_owner()
    journal.commit(
        expected_revision=owner.revision,
        writer_epoch=journal.writer_epoch,
        transition=JournalTransition(network_effect_inserts=(hydration,)),
    )
    try:
        revision = journal.load_owner().revision
        statements: list[str] = []
        journal.connection.set_trace_callback(statements.append)
        for effect_id in (UUID(int=999), hydration_id):
            with pytest.raises(JournalIntegrityError):
                journal.claim_membership_operation(effect_id)
        journal.connection.set_trace_callback(None)
        assert journal.load_owner().revision == revision
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_claim_and_resolution_use_one_ordered_transaction(tmp_path: Path) -> None:
    bootstrap, _, _ = await _open_with_membership(tmp_path)
    journal = bootstrap._journal
    try:
        statements: list[str] = []
        journal.connection.set_trace_callback(statements.append)
        _, ref = journal.claim_membership_operation(EFFECT_ID)
        journal.connection.set_trace_callback(None)
        normalized = [statement.strip().upper() for statement in statements]
        assert normalized.count("BEGIN IMMEDIATE") == 1
        assert normalized.count("COMMIT") == 1
        meta_index = next(
            index
            for index, statement in enumerate(normalized)
            if statement.startswith("UPDATE NIOINGESTMETA SET REVISION")
        )
        state_index = next(
            index
            for index, statement in enumerate(normalized)
            if statement.startswith("UPDATE NIOINGESTNETWORKEFFECT SET")
        )
        assert meta_index < state_index

        statements.clear()
        journal.connection.set_trace_callback(statements.append)
        journal.resolve_membership_operation(
            ref,
            MembershipOperationResolution.SUPERSEDE,
        )
        journal.connection.set_trace_callback(None)
        normalized = [statement.strip().upper() for statement in statements]
        assert normalized.count("BEGIN IMMEDIATE") == 1
        assert normalized.count("COMMIT") == 1
        meta_index = next(
            index
            for index, statement in enumerate(normalized)
            if statement.startswith("UPDATE NIOINGESTMETA SET REVISION")
        )
        delete_index = next(
            index
            for index, statement in enumerate(normalized)
            if statement.startswith("DELETE FROM NIOINGESTNETWORKEFFECT")
        )
        assert meta_index < delete_index
    finally:
        bootstrap.close()


@pytest.mark.parametrize("operation", ("claim", "retry", "supersede"))
@pytest.mark.parametrize(
    "tamper",
    ("request_ciphertext", "state_ciphertext", "attempt_ordinal"),
)
@pytest.mark.asyncio
async def test_membership_mutators_authenticate_target_before_dml(
    tmp_path: Path,
    operation: str,
    tamper: str,
) -> None:
    bootstrap, _, _ = await _open_with_membership(tmp_path)
    journal = bootstrap._journal
    try:
        ref = None
        if operation != "claim":
            _, ref = journal.claim_membership_operation(EFFECT_ID)
        if tamper.endswith("ciphertext"):
            original = journal.connection.execute(
                f"SELECT {tamper} FROM NioIngestNetworkEffect "
                "WHERE account_id=? AND effect_id=?",
                (ACCOUNT_ID, str(EFFECT_ID)),
            ).fetchone()[0]
            changed = bytearray(original)
            changed[-1] ^= 1
            journal.connection.execute(
                f"UPDATE NioIngestNetworkEffect SET {tamper}=? "
                "WHERE account_id=? AND effect_id=?",
                (bytes(changed), ACCOUNT_ID, str(EFFECT_ID)),
            )
        else:
            journal.connection.execute(
                "UPDATE NioIngestNetworkEffect SET attempt_ordinal=attempt_ordinal+1 "
                "WHERE account_id=? AND effect_id=?",
                (ACCOUNT_ID, str(EFFECT_ID)),
            )
        raw_before = tuple(
            journal.connection.execute(
                "SELECT * FROM NioIngestNetworkEffect "
                "WHERE account_id=? AND effect_id=?",
                (ACCOUNT_ID, str(EFFECT_ID)),
            ).fetchone()
        )
        revision = journal.load_owner().revision
        statements: list[str] = []
        journal.connection.set_trace_callback(statements.append)
        with pytest.raises(JournalIntegrityError):
            if operation == "claim":
                journal.claim_membership_operation(EFFECT_ID)
            else:
                assert ref is not None
                journal.resolve_membership_operation(
                    ref,
                    (
                        MembershipOperationResolution.RETRY
                        if operation == "retry"
                        else MembershipOperationResolution.SUPERSEDE
                    ),
                )
        journal.connection.set_trace_callback(None)
        raw_after = tuple(
            journal.connection.execute(
                "SELECT * FROM NioIngestNetworkEffect "
                "WHERE account_id=? AND effect_id=?",
                (ACCOUNT_ID, str(EFFECT_ID)),
            ).fetchone()
        )
        assert raw_after == raw_before
        assert journal.load_owner().revision == revision
        assert not any(
            statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "tamper",
    ("request_ciphertext", "state_ciphertext", "attempt_ordinal"),
)
@pytest.mark.asyncio
async def test_uncertain_status_authenticates_selected_target(
    tmp_path: Path,
    tamper: str,
) -> None:
    bootstrap, _, _ = await _open_with_membership(tmp_path)
    journal = bootstrap._journal
    try:
        journal.claim_membership_operation(EFFECT_ID)
        if tamper.endswith("ciphertext"):
            original = journal.connection.execute(
                f"SELECT {tamper} FROM NioIngestNetworkEffect "
                "WHERE account_id=? AND effect_id=?",
                (ACCOUNT_ID, str(EFFECT_ID)),
            ).fetchone()[0]
            changed = bytearray(original)
            changed[-1] ^= 1
            journal.connection.execute(
                f"UPDATE NioIngestNetworkEffect SET {tamper}=? "
                "WHERE account_id=? AND effect_id=?",
                (bytes(changed), ACCOUNT_ID, str(EFFECT_ID)),
            )
        else:
            journal.connection.execute(
                "UPDATE NioIngestNetworkEffect SET attempt_ordinal=attempt_ordinal+1 "
                "WHERE account_id=? AND effect_id=?",
                (ACCOUNT_ID, str(EFFECT_ID)),
            )
        revision = journal.load_owner().revision
        with pytest.raises(JournalIntegrityError):
            journal.uncertain_membership_operations(1)
        assert journal.load_owner().revision == revision
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_uncertain_membership_operations_use_bounded_keyset_pagination(
    tmp_path: Path,
) -> None:
    room_ids = tuple(f"!membership-{ordinal:03d}" for ordinal in range(257))
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=CLASSIC_SOURCE,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer(bootstrap, room_ids))
    journal = bootstrap._journal
    effects = tuple(
        _membership(
            bootstrap.stream_id,
            effect_id=UUID(int=ordinal + 1),
            room_id=room_id,
        )
        for ordinal, room_id in enumerate(room_ids)
    )
    try:
        insertion_order = (*effects[128:], *effects[:128])
        for chunk in (insertion_order[:256], insertion_order[256:]):
            owner = journal.load_owner()
            journal.commit(
                expected_revision=owner.revision,
                writer_epoch=journal.writer_epoch,
                transition=JournalTransition(network_effect_inserts=chunk),
            )
        dispatched = tuple(
            replace(
                effect,
                attempt_ordinal=1,
                membership_delivery_state=(
                    MembershipDeliveryState.DISPATCHED_UNCONFIRMED
                ),
            )
            for effect in effects
        )
        dispatch_order = (*dispatched[64:], *dispatched[:64])
        for chunk in (dispatch_order[:256], dispatch_order[256:]):
            owner = journal.load_owner()
            journal.commit(
                expected_revision=owner.revision,
                writer_epoch=journal.writer_epoch,
                transition=JournalTransition(network_effect_updates=chunk),
            )

        actual: list[MembershipOperationStatus] = []
        cursor = None
        while True:
            page = journal.uncertain_membership_operations(
                100,
                after_effect_id=cursor,
            )
            actual.extend(page)
            if len(page) < 100:
                break
            cursor = page[-1].ref.effect_id
        expected_ids = tuple(effect.request.effect_id for effect in effects)
        assert tuple(status.ref.effect_id for status in actual) == expected_ids
        assert len({status.ref.effect_id for status in actual}) == 257
        assert (
            journal.uncertain_membership_operations(
                1,
                after_effect_id=expected_ids[-1],
            )
            == ()
        )

        plan = tuple(
            row[3]
            for row in journal.connection.execute(
                "EXPLAIN QUERY PLAN SELECT * FROM NioIngestNetworkEffect "
                "WHERE account_id=? AND effect_kind='membership' "
                "AND membership_delivery_state='dispatched_unconfirmed' "
                "AND effect_id>? ORDER BY effect_id LIMIT ?",
                (ACCOUNT_ID, str(expected_ids[99]), 100),
            )
        )
        assert any("NioIngestNetworkEffect_uncertain" in detail for detail in plan)
        assert not any("TEMP B-TREE" in detail for detail in plan)
    finally:
        bootstrap.close()
