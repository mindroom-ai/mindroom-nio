from __future__ import annotations

import base64
import json
import sqlite3
from typing import TYPE_CHECKING
from uuid import UUID

from ..ingest._json import load_internal_json
from ..ingest.errors import JournalIntegrityError
from ..ingest.model import TransportKind
from ..ingest.ports import (
    NetworkRequest,
    StagedSourceResponse,
    _revalidated_staged_source_response,
)
from ..ingest.state import OwnerView, SourceState, StagedFrame
from ._sync_journal_preflight import _validate_source_cursor

if TYPE_CHECKING:
    from collections.abc import Mapping


_NORMALIZATION_VERSION = 1
_NETWORK_REQUEST_FIELDS = (
    "stream_id",
    "transport",
    "source_epoch",
    "request_id",
    "method",
    "path",
    "query",
    "body",
    "timeout_ms",
    "request_cursor_json",
)
_FRAME_FIELDS = (
    "normalization_version",
    "request",
    "response_body",
    "source_sha256",
)
# IngestionConfig caps the staged backlog at 256 frames. Reading one extra row
# keeps account identity classification bounded while detecting corrupt overflow.
_MAX_STAGED_FRAMES = 256
_FRAME_CLASSIFICATION_LIMIT = _MAX_STAGED_FRAMES + 1


def _canonical_internal(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _encoded_bytes(value: bytes | None) -> str | None:
    return base64.b64encode(value).decode("ascii") if value is not None else None


def _decoded_bytes(
    value: object,
    field_name: str,
    *,
    optional: bool = False,
) -> bytes | None:
    if value is None and optional:
        return None
    if type(value) is not str:
        raise ValueError(f"{field_name} must be a base64 string")
    try:
        return base64.b64decode(value, validate=True)
    except ValueError as error:
        raise ValueError(f"{field_name} must be valid base64") from error


def _network_request_to_dict(request: NetworkRequest) -> dict[str, object]:
    if type(request) is not NetworkRequest:
        raise TypeError("request must be NetworkRequest")
    return {
        "stream_id": str(request.stream_id),
        "transport": request.transport.value,
        "source_epoch": request.source_epoch,
        "request_id": request.request_id,
        "method": request.method,
        "path": request.path,
        "query": [[key, value] for key, value in request.query],
        "body": _encoded_bytes(request.body),
        "timeout_ms": request.timeout_ms,
        "request_cursor_json": _encoded_bytes(request.request_cursor_json),
    }


def _network_request_from_dict(value: object) -> NetworkRequest:
    if type(value) is not dict or tuple(value) != _NETWORK_REQUEST_FIELDS:
        raise ValueError("network request fields are not canonical")
    query = value["query"]
    if type(query) is not list:
        raise ValueError("network request query must be an array")
    pairs: list[tuple[str, str]] = []
    for pair in query:
        if type(pair) is not list or len(pair) != 2:
            raise ValueError("network request query entries are invalid")
        key, item = pair
        if type(key) is not str or type(item) is not str:
            raise ValueError("network request query entries are invalid")
        pairs.append((key, item))
    for name in ("source_epoch", "request_id", "timeout_ms"):
        if type(value[name]) is not int:
            raise ValueError("network request integer fields are invalid")
    for name in ("stream_id", "transport", "method", "path"):
        if type(value[name]) is not str:
            raise ValueError("network request string fields are invalid")
    cursor = _decoded_bytes(value["request_cursor_json"], "request cursor")
    assert cursor is not None
    try:
        request = NetworkRequest(
            UUID(value["stream_id"]),
            TransportKind(value["transport"]),
            value["source_epoch"],
            value["request_id"],
            value["method"],
            value["path"],
            tuple(pairs),
            _decoded_bytes(value["body"], "request body", optional=True),
            value["timeout_ms"],
            cursor,
        )
    except (TypeError, ValueError) as error:
        raise ValueError("network request is invalid") from error
    if value != _network_request_to_dict(request):
        raise ValueError("network request is not canonical")
    return request


def _frame_envelope(frame: StagedFrame) -> dict[str, object]:
    return {
        "normalization_version": _NORMALIZATION_VERSION,
        "request": _network_request_to_dict(frame.response.request),
        "response_body": _encoded_bytes(frame.response.response_body),
        "source_sha256": _encoded_bytes(frame.response.source_sha256),
    }


def _frame_response_from_envelope(value: object) -> StagedSourceResponse:
    if type(value) is not dict or tuple(value) != _FRAME_FIELDS:
        raise ValueError("frame authenticated envelope is invalid")
    if type(value["normalization_version"]) is not int or (
        value["normalization_version"] != _NORMALIZATION_VERSION
    ):
        raise ValueError("frame normalization version is invalid")
    body = _decoded_bytes(value["response_body"], "response body")
    digest = _decoded_bytes(value["source_sha256"], "source digest")
    assert body is not None
    assert digest is not None
    response = StagedSourceResponse(
        _network_request_from_dict(value["request"]),
        body,
        digest,
    )
    return response


def _source_header(source: SourceState) -> bytes:
    return _canonical_internal(
        [
            source.transport_kind.value,
            source.source_epoch,
            source.next_request_id,
            source.active,
        ]
    )


def _frame_header(frame: StagedFrame, staged_revision: int) -> bytes:
    request = frame.response.request
    return _canonical_internal(
        [
            request.source_epoch,
            request.request_id,
            staged_revision,
        ]
    )


class JournalRows:
    account_id: str
    device_id: str

    def _meta(self) -> sqlite3.Row:
        rows = self._execute(  # type: ignore[attr-defined]
            "SELECT * FROM NioIngestMeta"
        ).fetchall()
        if len(rows) != 1:
            raise JournalIntegrityError(
                "ingestion-v1 marker row cardinality is not one"
            )
        return rows[0]

    def _decode_owner_row(self, row: Mapping[str, object]) -> OwnerView:
        try:
            owner = OwnerView(
                row["account_id"],
                row["device_id"],
                row["schema_version"],
                UUID(row["stream_id"]),
                TransportKind(row["transport_kind"]),
                row["revision"],
                UUID(row["writer_epoch"]),
                row["next_source_epoch"],
            )
            if type(row["created_at_ns"]) is not int or row["created_at_ns"] < 0:
                raise ValueError("created_at_ns is invalid")
        except (AttributeError, TypeError, ValueError) as error:
            raise JournalIntegrityError("ingestion owner row is invalid") from error
        if owner.account_id != self.account_id or owner.device_id != self.device_id:
            raise JournalIntegrityError("ingestion owner identity changed")
        return owner

    def load_owner(self) -> OwnerView:
        with self._read():  # type: ignore[attr-defined]
            return self._decode_owner_row(self._meta())

    def _load_stage_snapshot(self) -> tuple[OwnerView, SourceState]:
        rows = self._execute(  # type: ignore[attr-defined]
            "SELECT m.*, s.account_id AS joined_source_account_id, "
            "s.source_epoch AS joined_source_epoch, "
            "s.cursor_ciphertext AS joined_cursor_ciphertext, "
            "s.cursor_sha256 AS joined_cursor_sha256, "
            "s.next_request_id AS joined_next_request_id, "
            "s.active AS joined_active FROM NioIngestMeta AS m "
            "CROSS JOIN NioIngestSourceState AS s"
        ).fetchall()
        if len(rows) != 1:
            raise JournalIntegrityError(
                "ingestion stage snapshot cardinality is not one"
            )
        row = rows[0]
        owner = self._decode_owner_row(row)
        source_row = {
            "account_id": row["joined_source_account_id"],
            "source_epoch": row["joined_source_epoch"],
            "cursor_ciphertext": row["joined_cursor_ciphertext"],
            "cursor_sha256": row["joined_cursor_sha256"],
            "next_request_id": row["joined_next_request_id"],
            "active": row["joined_active"],
        }
        return owner, self._decode_source_row(source_row, owner)

    def _decode_source_row(
        self,
        row: Mapping[str, object],
        owner: OwnerView,
    ) -> SourceState:
        try:
            active = row["active"]
            if type(active) is not int or active not in (0, 1):
                raise ValueError("source active column is invalid")
            clear = SourceState(
                row["source_epoch"],
                owner.transport_kind,
                b"",
                row["next_request_id"],
                bool(active),
            )
            cursor_json = self._codec.decrypt(  # type: ignore[attr-defined]
                "NioIngestSourceState",
                (self.account_id,),
                bytes(row["cursor_ciphertext"]),
                bytes(row["cursor_sha256"]),
                header=_source_header(clear),
            )
            source = SourceState(
                clear.source_epoch,
                clear.transport_kind,
                cursor_json,
                clear.next_request_id,
                clear.active,
            )
            _validate_source_cursor(source.transport_kind, source.cursor_json)
            if row["account_id"] != self.account_id:
                raise ValueError("source account_id changed")
            return source
        except JournalIntegrityError:
            raise
        except (KeyError, TypeError, ValueError) as error:
            raise JournalIntegrityError("persisted source state is invalid") from error

    def load_source(self) -> SourceState:
        with self._read():  # type: ignore[attr-defined]
            owner = self.load_owner()
            rows = self._execute(  # type: ignore[attr-defined]
                "SELECT * FROM NioIngestSourceState"
            ).fetchall()
            if len(rows) != 1:
                raise JournalIntegrityError(
                    "ingestion source row cardinality is not one"
                )
            return self._decode_source_row(rows[0], owner)

    def _write_source(self, source: SourceState) -> sqlite3.Cursor:
        _validate_source_cursor(source.transport_kind, source.cursor_json)
        ciphertext, digest = self._codec.seal(  # type: ignore[attr-defined]
            "NioIngestSourceState",
            (self.account_id,),
            source.cursor_json,
            header=_source_header(source),
        )
        return self._execute(  # type: ignore[attr-defined]
            "INSERT INTO NioIngestSourceState ("
            "account_id, source_epoch, cursor_ciphertext, cursor_sha256, "
            "next_request_id, active) VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(account_id) DO UPDATE SET "
            "source_epoch = excluded.source_epoch, "
            "cursor_ciphertext = excluded.cursor_ciphertext, "
            "cursor_sha256 = excluded.cursor_sha256, "
            "next_request_id = excluded.next_request_id, "
            "active = excluded.active",
            (
                self.account_id,
                source.source_epoch,
                ciphertext,
                digest,
                source.next_request_id,
                int(source.active),
            ),
        )

    @staticmethod
    def _parse_frame_id(row: Mapping[str, object]) -> tuple[str, UUID]:
        try:
            raw_frame_id = row["frame_id"]
            if type(raw_frame_id) is not str:
                raise TypeError
            stored_id = UUID(raw_frame_id)
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise JournalIntegrityError("persisted frame_id is invalid") from error
        return raw_frame_id, stored_id

    def _decode_frame_row(
        self,
        frame_id: UUID,
        row: Mapping[str, object],
        owner: OwnerView,
    ) -> StagedFrame:
        raw_frame_id, stored_id = self._parse_frame_id(row)
        if raw_frame_id != str(stored_id):
            raise JournalIntegrityError("persisted frame_id is not canonical")
        try:
            if stored_id != frame_id or row["account_id"] != self.account_id:
                raise ValueError("selected frame identity changed")
            source_epoch = row["source_epoch"]
            request_id = row["request_id"]
            staged_revision = row["staged_revision"]
            if any(
                type(value) is not int
                for value in (source_epoch, request_id, staged_revision)
            ):
                raise ValueError("frame ordering columns are invalid")
            header = _canonical_internal([source_epoch, request_id, staged_revision])
            payload = self._codec.decrypt(  # type: ignore[attr-defined]
                "NioIngestFrame",
                (frame_id,),
                bytes(row["payload_ciphertext"]),
                bytes(row["payload_sha256"]),
                header=header,
            )
            envelope = load_internal_json(payload, "frame authenticated envelope")
            response = _frame_response_from_envelope(envelope)
            frame = StagedFrame(frame_id, response, staged_revision)
            if payload != _canonical_internal(_frame_envelope(frame)):
                raise ValueError("frame authenticated envelope is not canonical")
            request = response.request
            if (
                request.stream_id != owner.stream_id
                or request.transport is not owner.transport_kind
            ):
                raise ValueError("frame request does not match journal owner")
            if (request.source_epoch, request.request_id) != (
                source_epoch,
                request_id,
            ):
                raise ValueError("frame columns do not match authenticated metadata")
            return frame
        except JournalIntegrityError:
            raise
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise JournalIntegrityError("persisted staged frame is invalid") from error

    def _classify_frame_ids(self) -> frozenset[UUID]:
        rows = self._execute(  # type: ignore[attr-defined]
            "SELECT frame_id FROM NioIngestFrame WHERE account_id = ? "
            "ORDER BY staged_revision, source_epoch, request_id, frame_id LIMIT ?",
            (self.account_id, _FRAME_CLASSIFICATION_LIMIT),
        ).fetchall()
        if len(rows) > _MAX_STAGED_FRAMES:
            raise JournalIntegrityError("staged frame count exceeds the 256 frame cap")

        classified = tuple(self._parse_frame_id(row) for row in rows)
        identities = [stored_id for _, stored_id in classified]
        if len(identities) != len(set(identities)):
            raise JournalIntegrityError("frame_id has multiple textual identities")
        for raw_frame_id, stored_id in classified:
            if raw_frame_id != str(stored_id):
                raise JournalIntegrityError("persisted frame_id is not canonical")
        return frozenset(identities)

    def _frame_row(self, frame_id: UUID) -> sqlite3.Row:
        row = self._execute(  # type: ignore[attr-defined]
            "SELECT * FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
            (self.account_id, str(frame_id)),
        ).fetchone()
        if row is None:
            raise JournalIntegrityError("classified frame_id row is missing")
        return row

    def _load_frame_with_owner(
        self,
        frame_id: UUID,
        owner: OwnerView,
    ) -> StagedFrame | None:
        if frame_id not in self._classify_frame_ids():
            return None
        return self._decode_frame_row(frame_id, self._frame_row(frame_id), owner)

    def load_frame(self, frame_id: UUID) -> StagedFrame | None:
        if type(frame_id) is not UUID:
            raise TypeError("frame_id must be UUID")
        with self._read():  # type: ignore[attr-defined]
            return self._load_frame_with_owner(frame_id, self.load_owner())

    def list_frames(self, limit: int) -> tuple[StagedFrame, ...]:
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError("frame limit must be an integer from 1 through 256")
        with self._read():  # type: ignore[attr-defined]
            owner = self.load_owner()
            self._classify_frame_ids()
            rows = self._execute(  # type: ignore[attr-defined]
                "SELECT * FROM NioIngestFrame WHERE account_id = ? "
                "ORDER BY staged_revision, source_epoch, request_id, frame_id LIMIT ?",
                (self.account_id, limit),
            ).fetchall()
            return tuple(
                self._decode_frame_row(self._parse_frame_id(row)[1], row, owner)
                for row in rows
            )

    def _write_frame(
        self,
        frame: StagedFrame,
        staged_revision: int,
    ) -> sqlite3.Cursor:
        stored = StagedFrame(
            frame.frame_id,
            _revalidated_staged_source_response(frame.response),
            staged_revision,
        )
        payload = _canonical_internal(_frame_envelope(stored))
        ciphertext, digest = self._codec.seal(  # type: ignore[attr-defined]
            "NioIngestFrame",
            (stored.frame_id,),
            payload,
            header=_frame_header(stored, staged_revision),
        )
        request = stored.response.request
        return self._execute(  # type: ignore[attr-defined]
            "INSERT INTO NioIngestFrame ("
            "account_id, frame_id, source_epoch, request_id, "
            "staged_revision, payload_ciphertext, payload_sha256"
            ") VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                self.account_id,
                str(stored.frame_id),
                request.source_epoch,
                request.request_id,
                staged_revision,
                ciphertext,
                digest,
            ),
        )
