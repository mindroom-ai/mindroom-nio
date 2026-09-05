"""Persisted Frame reads stay authenticated across reuse and ownership changes."""

import sqlite3
from dataclasses import replace
from uuid import uuid4

import pytest
from owned_capacity_test import (
    _baseline_session,
    _messages,
    _open_session,
    _settle_frame,
    _stage,
)

from nio.event_builders import ToDeviceMessage
from nio.exceptions import LocalProtocolError
from nio.ingest.errors import JournalIntegrityError
from nio.store._sync_journal_values import MaterializerLimits, MaterializeStatus


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field",
    [
        "payload",
        "payload_sha256",
        "source_epoch",
        "staged_revision",
        "room_materialized_revision",
        "callbacks_claimed_revision",
    ],
)
async def test_repeated_prepared_frame_reads_reject_changed_persisted_row(
    tmp_path, field
):
    session = await _baseline_session(tmp_path)
    journal = session._journal
    frame = _stage(session, _messages(2))
    try:
        session._materialize_oldest_frame(limits=MaterializerLimits())
        assert journal._oldest_prepared_frame_has_work(frame.frame_id)
        assert journal._oldest_prepared_frame_has_work(frame.frame_id)
        row = journal._frame_row(frame.frame_id)
        original = row[field]
        if field == "payload":
            replacement = original[:-1] + b"]"
        elif field == "payload_sha256":
            replacement = b"x" * 32
        else:
            replacement = journal.load_owner().revision + 1
        with sqlite3.connect(tmp_path / "journal.db") as external:
            external.execute(
                f"UPDATE NioIngestFrame SET {field} = ? WHERE frame_id = ?",
                (replacement, str(frame.frame_id)),
            )
        with pytest.raises(JournalIntegrityError):
            journal._oldest_prepared_frame_has_work(frame.frame_id)
        with sqlite3.connect(tmp_path / "journal.db") as external:
            external.execute(
                f"UPDATE NioIngestFrame SET {field} = ? WHERE frame_id = ?",
                (original, str(frame.frame_id)),
            )
        assert journal._oldest_prepared_frame_has_work(frame.frame_id)
        records = await _settle_frame(session, frame.frame_id)
        assert [record.event_id for record in records] == ["$message0", "$message1"]
    finally:
        await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_owner", ["revision", "stream"])
async def test_repeated_frame_read_checks_current_owner_before_reuse(
    tmp_path, changed_owner
):
    session = await _baseline_session(tmp_path)
    journal = session._journal
    frame = _stage(session, _messages(1))
    try:
        session._materialize_oldest_frame(limits=MaterializerLimits())
        row = journal._frame_row(frame.frame_id)
        owner = journal.load_owner()
        journal._decode_frame_state(frame.frame_id, row, owner)
        altered = {
            "revision": replace(owner, revision=row["staged_revision"]),
            "stream": replace(owner, stream_id=uuid4()),
        }[changed_owner]
        with pytest.raises(JournalIntegrityError):
            journal._decode_frame_state(
                frame.frame_id, row, altered, drain_header_authenticated=True
            )
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_rolled_back_frame_read_does_not_survive_restage_and_reopen(tmp_path):
    session = await _baseline_session(tmp_path)
    journal = session._journal
    prior_source = journal.load_source()
    observed = []

    class StageAborted(Exception):
        pass

    def abort_after_frame_insert(label):
        if label == "frame_insert":
            observed.extend(journal.list_frames(1))
            raise StageAborted

    journal.set_transition_statement_hook(abort_after_frame_insert)
    try:
        with pytest.raises(StageAborted):
            _stage(session, _messages(2))
        assert len(observed) == 1
        assert journal.load_source() == prior_source
        assert journal.list_frames(1) == ()
        journal.set_transition_statement_hook(None)
        staged = _stage(session, _messages(2))
        assert staged.frame_id == observed[0].frame_id
        assert journal.list_frames(1)[0].response == observed[0].response
        await session.close()
        with pytest.raises(LocalProtocolError):
            journal.load_frame(staged.frame_id)
        session = _open_session(tmp_path)
        assert session._journal.list_frames(1)[0].response == observed[0].response
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        records = await _settle_frame(session, staged.frame_id)
        assert [record.event_id for record in records] == ["$message0", "$message1"]
    finally:
        if not session._closed:
            await session.close()


@pytest.mark.asyncio
async def test_mutable_outbound_context_is_reloaded_from_persistence(tmp_path):
    session = await _baseline_session(tmp_path)
    try:
        olm = session._client.olm
        assert olm is not None
        olm.account.shared = True
        olm.uploaded_key_count = olm.account.max_one_time_keys
        olm.outgoing_to_device_messages.append(
            ToDeviceMessage(
                "org.example.test", "@other:example.org", "OTHER", {"value": "durable"}
            )
        )
        olm.save_account()
        _stage(
            session,
            {
                "next_batch": "s2",
                "rooms": {},
                "device_one_time_keys_count": {
                    "signed_curve25519": olm.account.max_one_time_keys
                },
            },
        )
        session._materialize_oldest_frame(limits=MaterializerLimits())
        first = session._journal._load_ready_outbound_maintenance()
        assert first is not None
        assert first.operation.context == {"subtype": "generic"}
        expected_body = first.operation.body_json
        first.operation.context["subtype"] = "changed"
        second = session._journal._load_ready_outbound_maintenance()
        assert second is not None
        assert second.operation.context == {"subtype": "generic"}
        assert second.operation.body_json == expected_body
        await session.close()
        session = _open_session(tmp_path)
        reopened = session._journal._load_ready_outbound_maintenance()
        assert reopened is not None
        assert reopened.operation.context == {"subtype": "generic"}
        assert reopened.operation.body_json == expected_body
    finally:
        await session.close()
