"""Authenticated pending Classic input survives bounded child delivery."""

import json
from dataclasses import replace
from hashlib import sha256
from uuid import uuid4

import pytest
from source_journal_test import CLASSIC_SOURCE, _open, _stage, _stage_proposal

from nio.ingest.errors import JournalIntegrityError
from nio.ingest.ports import StagedSourceResponse, _frame_id_for_response
from nio.ingest.source import canonical_json
from nio.ingest.state import StagedFrame

ROOM = "!room:example.org"


def _pending_input(opened):
    journal = opened._journal
    _stage(journal, proposal=_stage_proposal(journal, CLASSIC_SOURCE, 1))
    proposal = _stage_proposal(journal, CLASSIC_SOURCE, 2)
    original = proposal.frame.response
    body = json.loads(original.response_body)
    body["rooms"]["join"][ROOM]["timeline"].update(
        {"limited": True, "prev_batch": "tail"}
    )
    payload = canonical_json(body)
    response = StagedSourceResponse(original.request, payload, sha256(payload).digest())
    cursor = canonical_json(
        {
            "next_batch": "s1",
            "recovery": {
                "source_sha256": response.source_sha256.hex(),
                "rooms": [ROOM],
                "phase": "prologue",
                "room_index": 0,
                "page_count": 0,
                "token": "s1",
                "seen_tokens": ["s1"],
                "previous_event_ids": [],
            },
        }
    )
    return response, cursor


def test_journal_without_pending_recovery_has_no_retained_response(tmp_path):
    opened = _open(tmp_path)
    try:
        assert opened._journal._load_classic_recovery() is None
    finally:
        opened.close()


def test_pending_input_survives_reopen_without_advancing_global_cursor(tmp_path):
    opened = _open(tmp_path)
    response, cursor = _pending_input(opened)
    prior = opened._journal.load_source()
    revision = opened._journal.load_owner().revision
    try:
        opened._journal._begin_classic_recovery(response=response, cursor_json=cursor)
        assert opened._journal.load_source() == replace(prior, cursor_json=cursor)
        assert opened._journal.load_owner().revision == revision + 1
    finally:
        opened.close()
    reopened = _open(tmp_path)
    try:
        assert reopened._journal._load_classic_recovery() == response
        assert reopened._journal.load_source().cursor_json == cursor
    finally:
        reopened.close()


@pytest.mark.parametrize("field", ["owner", "epoch", "request", "digest", "since"])
def test_begin_rejects_input_that_does_not_match_current_source(tmp_path, field):
    opened = _open(tmp_path)
    response, cursor = _pending_input(opened)
    journal = opened._journal
    prior = journal.load_source()
    revision = journal.load_owner().revision
    if field in {"owner", "epoch", "request", "since"}:
        changes = {
            "owner": {"stream_id": uuid4()},
            "epoch": {"source_epoch": prior.source_epoch + 1},
            "request": {"request_id": prior.next_request_id + 1},
            "since": {"request_cursor_json": b'{"next_batch":"wrong"}'},
        }
        response = replace(
            response, request=replace(response.request, **changes[field])
        )
    else:
        value = json.loads(cursor)
        value["recovery"]["source_sha256"] = "00" * 32
        cursor = canonical_json(value)
    try:
        with pytest.raises(JournalIntegrityError):
            journal._begin_classic_recovery(response=response, cursor_json=cursor)
        assert journal.load_source() == prior
        assert journal.load_owner().revision == revision
        assert journal._load_classic_recovery() is None
    finally:
        opened.close()


@pytest.mark.parametrize(
    "boundary", ["recovery_insert", "source_state_upsert", "meta_revision_epoch_cas"]
)
def test_begin_rolls_back_input_and_cursor_together(tmp_path, boundary):
    opened = _open(tmp_path)
    response, cursor = _pending_input(opened)
    journal = opened._journal
    prior = journal.load_source()
    revision = journal.load_owner().revision

    def interrupt(label):
        if label == boundary:
            raise RuntimeError("interrupted recovery begin")

    try:
        journal.set_transition_statement_hook(interrupt)
        with pytest.raises(RuntimeError, match="interrupted recovery begin"):
            journal._begin_classic_recovery(response=response, cursor_json=cursor)
        journal.set_transition_statement_hook(None)
        assert journal.load_source() == prior
        assert journal.load_owner().revision == revision
        assert journal._load_classic_recovery() is None
    finally:
        opened.close()


@pytest.mark.parametrize("column", ["payload_sha256", "source_epoch", "request_id"])
def test_load_rejects_changed_recovery_envelope(tmp_path, column):
    opened = _open(tmp_path)
    response, cursor = _pending_input(opened)
    journal = opened._journal
    try:
        journal._begin_classic_recovery(response=response, cursor_json=cursor)
        value = bytes(32) if column == "payload_sha256" else 999
        with journal._transaction():
            journal._execute(f"UPDATE NioIngestRecovery SET {column} = ?", (value,))
        with pytest.raises(JournalIntegrityError):
            journal._load_classic_recovery()
    finally:
        opened.close()


def test_missing_retained_input_does_not_silently_resume_sync(tmp_path):
    opened = _open(tmp_path)
    response, cursor = _pending_input(opened)
    journal = opened._journal
    try:
        journal._begin_classic_recovery(response=response, cursor_json=cursor)
        with journal._transaction():
            journal._execute("DELETE FROM NioIngestRecovery")
        with pytest.raises(JournalIntegrityError):
            journal._load_classic_recovery()
    finally:
        opened.close()


@pytest.mark.parametrize("interrupt", [False, True])
def test_final_child_staging_deletes_pending_input_atomically(tmp_path, interrupt):
    opened = _open(tmp_path)
    response, cursor = _pending_input(opened)
    journal = opened._journal
    try:
        journal._begin_classic_recovery(response=response, cursor_json=cursor)
        value = json.loads(cursor)
        value["recovery"].update({"phase": "tail", "room_index": 1})
        prior = replace(journal.load_source(), cursor_json=canonical_json(value))
        with journal._transaction():
            journal._write_source(prior, journal.load_owner())
        response = replace(
            response,
            request=replace(
                response.request,
                request_cursor_json=prior.cursor_json,
                request_id=prior.next_request_id,
            ),
        )
        frame = StagedFrame(
            _frame_id_for_response(response.request, response.source_sha256), response
        )
        successor = replace(
            prior,
            cursor_json=b'{"next_batch":"s2"}',
            next_request_id=prior.next_request_id + 1,
        )

        def fail_after_delete(label):
            if label == "recovery_delete":
                raise RuntimeError("interrupted recovery completion")

        if interrupt:
            journal.set_transition_statement_hook(fail_after_delete)
            with pytest.raises(RuntimeError, match="interrupted recovery completion"):
                journal.stage_source_response(source=successor, frame=frame)
            journal.set_transition_statement_hook(None)
            assert journal.load_source() == prior
            assert journal._load_classic_recovery() is not None
            assert journal.load_frame(frame.frame_id) is None
        else:
            journal.stage_source_response(source=successor, frame=frame)
            assert journal.load_source() == successor
            assert journal._load_classic_recovery() is None
            assert journal.load_frame(frame.frame_id) == replace(
                frame, staged_revision=journal.load_owner().revision
            )
    finally:
        opened.close()
