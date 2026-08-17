import hashlib
import json
from typing import Any
from uuid import UUID

from ..ingest._json import load_internal_json
from ..ingest.errors import JournalIntegrityError
from ..ingest.model import TransportKind
from ..ingest.sliding import _sliding_cursor_from_json, canonical_sliding_cursor
from ..ingest.source import _classic_cursor_from_json, canonical_classic_cursor
from ..ingest.state import SourceState


def _canonical_internal(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


_STORED_ROWS = {
    "NioIngestSourceState": "source source_epoch next_request_id active",
    "NioIngestFrame": "frame frame_id source_epoch request_id staged_revision",
    "NioIngestFrameDrainHeader": "frame frame_id source_epoch request_id staged_revision payload_sha256 payload_length room_materialized_revision callbacks_claimed_revision",
    "NioIngestRoomAggregate": "aggregate room_id updated_revision intent_kind",
    "NioIngestWork": "work work_id kind status frame_id room_id membership_epoch room_sequence ready_revision ready_ordinal created_revision",
}


def _row(
    owner: tuple[str, UUID, TransportKind],
    table: str,
    value: Any,
    digest: object = None,
    header: bytes | tuple[object, ...] = b"",
) -> Any:
    if digest is not None:
        envelope = load_internal_json(value, "stored payload")
        payload, value = value, _canonical_internal(dict.get(envelope, "value"))
    kind, *fields = _STORED_ROWS[table].split()
    clear = [*header] if isinstance(header, tuple) else load_internal_json(header, kind)
    prefix = _canonical_internal(
        {
            "schema_version": 1,
            "row_kind": kind,
            "account_id": owner[0],
            "stream_id": str(owner[1]),
            "transport_kind": owner[2].value,
            **dict(zip(fields, clear, strict=True)),
        }
    )
    if value is None:
        return hashlib.sha256(prefix).digest()
    expected = prefix[:-1] + b',"value":' + value + b"}"
    if digest is None:
        return expected, hashlib.sha256(expected).digest()
    if hashlib.sha256(payload).digest() != digest or payload != expected:
        raise ValueError("stored payload is not canonical")
    return value


def _source_header(source: SourceState) -> bytes:
    return _canonical_internal(
        [
            source.source_epoch,
            source.next_request_id,
            source.active,
        ]
    )


def _validate_source_cursor(
    transport_kind: TransportKind,
    cursor_json: bytes,
) -> None:
    try:
        if transport_kind is TransportKind.CLASSIC:
            canonical = canonical_classic_cursor(_classic_cursor_from_json(cursor_json))
        else:
            canonical = canonical_sliding_cursor(_sliding_cursor_from_json(cursor_json))
    except (TypeError, ValueError) as error:
        raise JournalIntegrityError(
            f"persisted {transport_kind.value} source cursor is invalid"
        ) from error
    if canonical != cursor_json:
        raise JournalIntegrityError(
            f"persisted {transport_kind.value} source cursor is not canonical"
        )
