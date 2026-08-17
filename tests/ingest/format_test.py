from uuid import UUID

import pytest

from nio.ingest.model import TransportKind
from nio.ingest.state import SourceState


def test_row_format_authenticates_literal_canonical_payload() -> None:
    from nio.store._sync_journal_format import _row

    owner = (
        "@alice:example.org",
        UUID("00000000-0000-0000-0000-000000000001"),
        TransportKind.CLASSIC,
    )
    expected = (
        b'{"schema_version":1,"row_kind":"source",'
        b'"account_id":"@alice:example.org",'
        b'"stream_id":"00000000-0000-0000-0000-000000000001",'
        b'"transport_kind":"classic","source_epoch":2,'
        b'"next_request_id":3,"active":true,"value":{"a":1}}'
    )

    assert _row(
        owner,
        "NioIngestSourceState",
        b'{"a":1}',
        header=(2, 3, True),
    ) == (
        expected,
        bytes.fromhex(
            "7e96053d59c99243953f2f1e7dc7b33e" "fc347dce598e630b6a64903fc2c89b6a"
        ),
    )
    with pytest.raises(ValueError, match="stored payload is not canonical"):
        _row(
            owner,
            "NioIngestSourceState",
            expected,
            digest=bytes(32),
            header=(2, 3, True),
        )


def test_source_header_uses_the_authenticated_source_frontier() -> None:
    from nio.store._sync_journal_format import _source_header

    source = SourceState(
        source_epoch=7,
        transport_kind=TransportKind.SLIDING,
        cursor_json=b"null",
        next_request_id=11,
        active=False,
    )

    assert _source_header(source) == b"[7,11,false]"


def test_journal_components_share_the_format_primitives() -> None:
    from nio.store import (
        _sync_journal_format,
        _sync_journal_plan,
        _sync_journal_preflight,
        _sync_journal_rows,
    )

    assert (
        _sync_journal_preflight._canonical_internal
        is _sync_journal_format._canonical_internal
    )
    assert _sync_journal_preflight._row is _sync_journal_format._row
    assert _sync_journal_preflight._source_header is _sync_journal_format._source_header
    assert (
        _sync_journal_rows._canonical_internal
        is _sync_journal_format._canonical_internal
    )
    assert _sync_journal_rows._row is _sync_journal_format._row
    assert _sync_journal_rows._source_header is _sync_journal_format._source_header
    assert (
        _sync_journal_plan._canonical_internal
        is _sync_journal_format._canonical_internal
    )
    assert _sync_journal_plan._row is _sync_journal_format._row
