from __future__ import annotations

import base64
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal, NamedTuple, cast
from uuid import UUID

from ..ingest._json import load_internal_json
from ..ingest.diagnostic import DiagnosticIngestionScope
from ..ingest.errors import JournalIntegrityError
from ..ingest.model import TransportKind
from ..ingest.state import OwnerView
from ._sync_journal_preflight import _row
from ._sync_journal_rows import _canonical_internal

if TYPE_CHECKING:
    from collections.abc import Mapping


MAX_LIST_OBSERVATIONS = 4_096
MAX_EVENT_RECEIPTS = 4_096
MAX_EVENT_OCCURRENCES = 8_192
MAX_SOURCE_ROTATIONS = 64


def _b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _uuid(value: object, field_name: str) -> UUID:
    if type(value) is not str:
        raise TypeError(f"{field_name} must be canonical UUID text")
    parsed = UUID(value)
    if value != str(parsed):
        raise ValueError(f"{field_name} must be canonical UUID text")
    return parsed


def _digest(value: object, field_name: str) -> bytes:
    if type(value) is not bytes or len(value) != 32:
        raise TypeError(f"{field_name} must be a 32-byte digest")
    return value


def _revision(value: object, owner: OwnerView, field_name: str) -> int:
    if type(value) is not int or not 1 <= value <= owner.revision:
        raise ValueError(f"{field_name} is outside the authenticated revision")
    return value


def _canonical_value(payload: bytes, label: str) -> bytes:
    value = load_internal_json(payload, label)
    if type(value) is not dict or _canonical_internal(value) != payload:
        raise ValueError(f"{label} must be a canonical object")
    return payload


def canonical_diagnostic_scope(scope: DiagnosticIngestionScope) -> bytes:
    return _canonical_internal(
        {
            "account_id": scope.account_id,
            "delivery_room_id": scope.delivery_room_id,
            "control_room_id": scope.control_room_id,
            "list_name": scope.list_name,
            "range_start": scope.range_start,
            "range_end": scope.range_end,
            "request_config_sha256": _b64(scope.request_config_sha256),
        }
    )


def _scope_header(scope: DiagnosticIngestionScope) -> tuple[object, ...]:
    return (
        scope.delivery_room_id,
        scope.control_room_id,
        scope.list_name,
        scope.range_start,
        scope.range_end,
        _b64(scope.request_config_sha256),
        0,
    )


def diagnostic_scope_row(
    owner: OwnerView | tuple[str, UUID, TransportKind],
    scope: DiagnosticIngestionScope,
) -> tuple[bytes, bytes]:
    bound: tuple[str, UUID, TransportKind] = (
        (owner.account_id, owner.stream_id, owner.transport_kind)
        if isinstance(owner, OwnerView)
        else owner
    )
    return _row(
        bound,
        "NioIngestDiagnosticScope",
        canonical_diagnostic_scope(scope),
        header=_scope_header(scope),
    )


@dataclass(frozen=True, slots=True)
class ListObservationValue:
    observation_id: UUID
    frame_id: UUID
    source_epoch: int
    request_id: int
    list_name: str
    transition: Literal["seed", "enter", "evict", "reenter", "replace"]
    target_present: bool
    control_present: bool
    created_revision: int
    payload_json: bytes


@dataclass(frozen=True, slots=True)
class EventReceiptValue:
    receipt_id: UUID
    room_id: str
    event_id: str
    stable_event_sha256: bytes
    first_frame_id: UUID
    first_source_sha256: bytes
    work_id: UUID | None
    fate: Literal[
        "context", "control", "self_control", "ready", "outstanding", "acknowledged"
    ]
    delivery_sequence: int | None
    delivery_batch_sha256: bytes | None
    acknowledged_revision: int | None
    created_revision: int
    updated_revision: int
    payload_json: bytes


@dataclass(frozen=True, slots=True)
class EventOccurrenceValue:
    occurrence_id: UUID
    receipt_id: UUID
    frame_id: UUID
    source_epoch: int
    request_id: int
    source_sha256: bytes
    provenance: Literal["live", "history"]
    disposition: Literal[
        "application", "context", "control", "self_control", "duplicate"
    ]
    created_revision: int
    payload_json: bytes


@dataclass(frozen=True, slots=True)
class SourceRotationValue:
    successor_source_epoch: int
    successor_request_id: int
    reason: Literal["reopen", "unknown_pos"]
    predecessor_source_epoch: int
    predecessor_request_id: int
    predecessor_cursor_sha256: bytes
    predecessor_connection_sha256: bytes
    predecessor_pos_present: bool
    successor_cursor_sha256: bytes
    successor_connection_sha256: bytes
    successor_pos_present: bool
    first_successor_request_sha256: bytes | None
    first_successor_frame_id: UUID | None
    first_successor_source_sha256: bytes | None
    created_revision: int
    payload_json: bytes


class DiagnosticInventory(NamedTuple):
    list_observations: tuple[ListObservationValue, ...]
    event_receipts: tuple[EventReceiptValue, ...]
    event_occurrences: tuple[EventOccurrenceValue, ...]
    source_rotations: tuple[SourceRotationValue, ...]


class DiagnosticJournalRows:
    account_id: str

    def _diagnostic_execute(self, statement: str, parameters: tuple[object, ...] = ()):
        return getattr(self, "_execute")(statement, parameters)

    def _diagnostic_payload(self, *args: object, **kwargs: object):
        return getattr(self, "_payload")(*args, **kwargs)

    def _diagnostic_read(self):
        return getattr(self, "_read")()

    def _diagnostic_owner(self) -> OwnerView:
        return getattr(self, "load_owner")()

    def _decode_scope_row(
        self,
        row: Mapping[str, object],
        owner: OwnerView,
    ) -> DiagnosticIngestionScope:
        try:
            if row["account_id"] != self.account_id or row["created_revision"] != 0:
                raise ValueError("scope ownership or revision is invalid")
            scope = DiagnosticIngestionScope(
                cast("str", row["account_id"]),
                cast("str", row["delivery_room_id"]),
                cast("str", row["control_room_id"]),
                cast("str", row["list_name"]),
                cast("int", row["range_start"]),
                cast("int", row["range_end"]),
                _digest(row["request_config_sha256"], "request_config_sha256"),
            )
            payload = self._diagnostic_payload(
                owner,
                "NioIngestDiagnosticScope",
                row["payload"],
                row["payload_sha256"],
                header=_canonical_internal(_scope_header(scope)),
            )
            if payload != canonical_diagnostic_scope(scope):
                raise ValueError("scope payload does not match clear columns")
            return scope
        except JournalIntegrityError:
            raise
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise JournalIntegrityError(
                "persisted diagnostic scope is invalid"
            ) from error

    def _load_diagnostic_scope(
        self, owner: OwnerView
    ) -> DiagnosticIngestionScope | None:
        rows = self._diagnostic_execute(
            "SELECT * FROM NioIngestDiagnosticScope LIMIT 2"
        ).fetchall()
        if len(rows) > 1:
            raise JournalIntegrityError("diagnostic scope row cardinality exceeds one")
        return None if not rows else self._decode_scope_row(rows[0], owner)

    def load_diagnostic_scope(self) -> DiagnosticIngestionScope | None:
        with self._diagnostic_read():
            owner = self._diagnostic_owner()
            return self._load_diagnostic_scope(owner)

    def _load_list_observations(
        self, owner: OwnerView
    ) -> tuple[ListObservationValue, ...]:
        rows = self._diagnostic_execute(
            "SELECT * FROM NioIngestListObservation " "ORDER BY observation_id LIMIT ?",
            (MAX_LIST_OBSERVATIONS + 1,),
        ).fetchall()
        if len(rows) > MAX_LIST_OBSERVATIONS:
            raise JournalIntegrityError("list observation inventory exceeds its cap")
        values: list[ListObservationValue] = []
        for row in rows:
            try:
                if row["account_id"] != self.account_id:
                    raise ValueError("list observation owner is invalid")
                observation_id = _uuid(row["observation_id"], "observation_id")
                frame_id = _uuid(row["frame_id"], "frame_id")
                source_epoch, request_id = row["source_epoch"], row["request_id"]
                if (
                    type(source_epoch) is not int
                    or source_epoch < 0
                    or type(request_id) is not int
                    or request_id < 0
                    or type(row["list_name"]) is not str
                    or not row["list_name"]
                    or row["transition"]
                    not in ("seed", "enter", "evict", "reenter", "replace")
                    or type(row["target_present"]) is not int
                    or row["target_present"] not in (0, 1)
                    or type(row["control_present"]) is not int
                    or row["control_present"] not in (0, 1)
                ):
                    raise ValueError("list observation clear columns are invalid")
                created = _revision(row["created_revision"], owner, "created_revision")
                clear = (
                    str(observation_id),
                    str(frame_id),
                    source_epoch,
                    request_id,
                    row["list_name"],
                    row["transition"],
                    bool(row["target_present"]),
                    bool(row["control_present"]),
                    created,
                )
                payload = self._diagnostic_payload(
                    owner,
                    "NioIngestListObservation",
                    row["payload"],
                    row["payload_sha256"],
                    header=_canonical_internal(clear),
                )
                values.append(
                    ListObservationValue(
                        observation_id,
                        frame_id,
                        source_epoch,
                        request_id,
                        cast("str", row["list_name"]),
                        cast(
                            "Literal['seed', 'enter', 'evict', 'reenter', 'replace']",
                            row["transition"],
                        ),
                        bool(row["target_present"]),
                        bool(row["control_present"]),
                        created,
                        _canonical_value(payload, "list observation payload"),
                    )
                )
            except JournalIntegrityError:
                raise
            except (AttributeError, KeyError, TypeError, ValueError) as error:
                raise JournalIntegrityError(
                    "persisted list observation is invalid"
                ) from error
        return tuple(values)

    def _load_event_receipts(self, owner: OwnerView) -> tuple[EventReceiptValue, ...]:
        rows = self._diagnostic_execute(
            "SELECT * FROM NioIngestEventReceipt ORDER BY receipt_id LIMIT ?",
            (MAX_EVENT_RECEIPTS + 1,),
        ).fetchall()
        if len(rows) > MAX_EVENT_RECEIPTS:
            raise JournalIntegrityError("event receipt inventory exceeds its cap")
        values: list[EventReceiptValue] = []
        for row in rows:
            try:
                if row["account_id"] != self.account_id:
                    raise ValueError("event receipt owner is invalid")
                receipt_id = _uuid(row["receipt_id"], "receipt_id")
                first_frame_id = _uuid(row["first_frame_id"], "first_frame_id")
                work_id = (
                    None if row["work_id"] is None else _uuid(row["work_id"], "work_id")
                )
                fate = row["fate"]
                if (
                    type(row["room_id"]) is not str
                    or not row["room_id"]
                    or type(row["event_id"]) is not str
                    or not row["event_id"]
                    or fate
                    not in (
                        "context",
                        "control",
                        "self_control",
                        "ready",
                        "outstanding",
                        "acknowledged",
                    )
                ):
                    raise ValueError("event receipt clear columns are invalid")
                stable = _digest(row["stable_event_sha256"], "stable_event_sha256")
                first_source = _digest(
                    row["first_source_sha256"], "first_source_sha256"
                )
                created = _revision(row["created_revision"], owner, "created_revision")
                updated = _revision(row["updated_revision"], owner, "updated_revision")
                if updated < created:
                    raise ValueError("receipt revision order is invalid")
                sequence = row["delivery_sequence"]
                batch = row["delivery_batch_sha256"]
                acknowledged = row["acknowledged_revision"]
                if sequence is not None and (type(sequence) is not int or sequence < 0):
                    raise ValueError("delivery sequence is invalid")
                if batch is not None:
                    batch = _digest(batch, "delivery_batch_sha256")
                if acknowledged is not None:
                    acknowledged = _revision(
                        acknowledged, owner, "acknowledged_revision"
                    )
                clear = (
                    str(receipt_id),
                    row["room_id"],
                    row["event_id"],
                    _b64(stable),
                    str(first_frame_id),
                    _b64(first_source),
                    None if work_id is None else str(work_id),
                    fate,
                    sequence,
                    None if batch is None else _b64(batch),
                    acknowledged,
                    created,
                    updated,
                )
                payload = self._diagnostic_payload(
                    owner,
                    "NioIngestEventReceipt",
                    row["payload"],
                    row["payload_sha256"],
                    header=_canonical_internal(clear),
                )
                values.append(
                    EventReceiptValue(
                        receipt_id,
                        cast("str", row["room_id"]),
                        cast("str", row["event_id"]),
                        stable,
                        first_frame_id,
                        first_source,
                        work_id,
                        cast(
                            "Literal['context', 'control', 'self_control', 'ready', 'outstanding', 'acknowledged']",
                            fate,
                        ),
                        cast("int | None", sequence),
                        cast("bytes | None", batch),
                        cast("int | None", acknowledged),
                        created,
                        updated,
                        _canonical_value(payload, "event receipt payload"),
                    )
                )
            except JournalIntegrityError:
                raise
            except (AttributeError, KeyError, TypeError, ValueError) as error:
                raise JournalIntegrityError(
                    "persisted event receipt is invalid"
                ) from error
        return tuple(values)

    def _load_event_occurrences(
        self, owner: OwnerView
    ) -> tuple[EventOccurrenceValue, ...]:
        rows = self._diagnostic_execute(
            "SELECT * FROM NioIngestEventOccurrence ORDER BY occurrence_id LIMIT ?",
            (MAX_EVENT_OCCURRENCES + 1,),
        ).fetchall()
        if len(rows) > MAX_EVENT_OCCURRENCES:
            raise JournalIntegrityError("event occurrence inventory exceeds its cap")
        values: list[EventOccurrenceValue] = []
        for row in rows:
            try:
                if row["account_id"] != self.account_id:
                    raise ValueError("event occurrence owner is invalid")
                occurrence_id = _uuid(row["occurrence_id"], "occurrence_id")
                receipt_id = _uuid(row["receipt_id"], "receipt_id")
                frame_id = _uuid(row["frame_id"], "frame_id")
                source_epoch, request_id = row["source_epoch"], row["request_id"]
                source_digest = _digest(row["source_sha256"], "source_sha256")
                provenance, disposition = row["provenance"], row["disposition"]
                if (
                    type(source_epoch) is not int
                    or source_epoch < 0
                    or type(request_id) is not int
                    or request_id < 0
                    or provenance not in ("live", "history")
                    or disposition
                    not in (
                        "application",
                        "context",
                        "control",
                        "self_control",
                        "duplicate",
                    )
                ):
                    raise ValueError("event occurrence clear columns are invalid")
                created = _revision(row["created_revision"], owner, "created_revision")
                clear = (
                    str(occurrence_id),
                    str(receipt_id),
                    str(frame_id),
                    source_epoch,
                    request_id,
                    _b64(source_digest),
                    provenance,
                    disposition,
                    created,
                )
                payload = self._diagnostic_payload(
                    owner,
                    "NioIngestEventOccurrence",
                    row["payload"],
                    row["payload_sha256"],
                    header=_canonical_internal(clear),
                )
                values.append(
                    EventOccurrenceValue(
                        occurrence_id,
                        receipt_id,
                        frame_id,
                        source_epoch,
                        request_id,
                        source_digest,
                        cast("Literal['live', 'history']", provenance),
                        cast(
                            "Literal['application', 'context', 'control', 'self_control', 'duplicate']",
                            disposition,
                        ),
                        created,
                        _canonical_value(payload, "event occurrence payload"),
                    )
                )
            except JournalIntegrityError:
                raise
            except (AttributeError, KeyError, TypeError, ValueError) as error:
                raise JournalIntegrityError(
                    "persisted event occurrence is invalid"
                ) from error
        return tuple(values)

    def _load_source_rotations(
        self, owner: OwnerView
    ) -> tuple[SourceRotationValue, ...]:
        rows = self._diagnostic_execute(
            "SELECT * FROM NioIngestSourceRotation "
            "ORDER BY successor_source_epoch, successor_request_id LIMIT ?",
            (MAX_SOURCE_ROTATIONS + 1,),
        ).fetchall()
        if len(rows) > MAX_SOURCE_ROTATIONS:
            raise JournalIntegrityError("source rotation inventory exceeds its cap")
        values: list[SourceRotationValue] = []
        for row in rows:
            try:
                if row["account_id"] != self.account_id:
                    raise ValueError("source rotation owner is invalid")
                successor_epoch = row["successor_source_epoch"]
                successor_request = row["successor_request_id"]
                predecessor_epoch = row["predecessor_source_epoch"]
                predecessor_request = row["predecessor_request_id"]
                reason = row["reason"]
                if (
                    type(successor_epoch) is not int
                    or successor_epoch < 1
                    or successor_request != 0
                    or type(predecessor_epoch) is not int
                    or not 0 <= predecessor_epoch < successor_epoch
                    or type(predecessor_request) is not int
                    or predecessor_request < 0
                    or reason not in ("reopen", "unknown_pos")
                ):
                    raise ValueError("source rotation clear columns are invalid")
                predecessor_cursor = _digest(
                    row["predecessor_cursor_sha256"], "predecessor_cursor_sha256"
                )
                predecessor_connection = _digest(
                    row["predecessor_connection_sha256"],
                    "predecessor_connection_sha256",
                )
                successor_cursor = _digest(
                    row["successor_cursor_sha256"], "successor_cursor_sha256"
                )
                successor_connection = _digest(
                    row["successor_connection_sha256"],
                    "successor_connection_sha256",
                )
                before_pos, after_pos = (
                    row["predecessor_pos_present"],
                    row["successor_pos_present"],
                )
                if before_pos not in (0, 1) or after_pos != 0:
                    raise ValueError("source rotation position flags are invalid")
                request_digest = row["first_successor_request_sha256"]
                frame_value = row["first_successor_frame_id"]
                source_digest = row["first_successor_source_sha256"]
                bindings = request_digest, frame_value, source_digest
                if any(value is None for value in bindings) != all(
                    value is None for value in bindings
                ):
                    raise ValueError("source rotation bindings are partial")
                if request_digest is not None:
                    request_digest = _digest(
                        request_digest, "first_successor_request_sha256"
                    )
                    source_digest = _digest(
                        source_digest, "first_successor_source_sha256"
                    )
                    frame_id = _uuid(frame_value, "first_successor_frame_id")
                else:
                    frame_id = None
                created = _revision(row["created_revision"], owner, "created_revision")
                clear = (
                    successor_epoch,
                    successor_request,
                    reason,
                    predecessor_epoch,
                    predecessor_request,
                    _b64(predecessor_cursor),
                    _b64(predecessor_connection),
                    bool(before_pos),
                    _b64(successor_cursor),
                    _b64(successor_connection),
                    False,
                    None if request_digest is None else _b64(request_digest),
                    None if frame_id is None else str(frame_id),
                    None if source_digest is None else _b64(source_digest),
                    created,
                )
                payload = self._diagnostic_payload(
                    owner,
                    "NioIngestSourceRotation",
                    row["payload"],
                    row["payload_sha256"],
                    header=_canonical_internal(clear),
                )
                values.append(
                    SourceRotationValue(
                        successor_epoch,
                        successor_request,
                        cast("Literal['reopen', 'unknown_pos']", reason),
                        predecessor_epoch,
                        predecessor_request,
                        predecessor_cursor,
                        predecessor_connection,
                        bool(before_pos),
                        successor_cursor,
                        successor_connection,
                        False,
                        cast("bytes | None", request_digest),
                        frame_id,
                        cast("bytes | None", source_digest),
                        created,
                        _canonical_value(payload, "source rotation payload"),
                    )
                )
            except JournalIntegrityError:
                raise
            except (AttributeError, KeyError, TypeError, ValueError) as error:
                raise JournalIntegrityError(
                    "persisted source rotation is invalid"
                ) from error
        return tuple(values)

    def _load_diagnostic_inventory(self, owner: OwnerView) -> DiagnosticInventory:
        return DiagnosticInventory(
            self._load_list_observations(owner),
            self._load_event_receipts(owner),
            self._load_event_occurrences(owner),
            self._load_source_rotations(owner),
        )

    @staticmethod
    def _require_diagnostic_capacity(
        inventory: DiagnosticInventory,
        *,
        list_observations: int = 0,
        event_receipts: int = 0,
        event_occurrences: int = 0,
        source_rotations: int = 0,
    ) -> None:
        prospective = (
            (
                len(inventory.list_observations),
                list_observations,
                MAX_LIST_OBSERVATIONS,
                "list observation",
            ),
            (
                len(inventory.event_receipts),
                event_receipts,
                MAX_EVENT_RECEIPTS,
                "event receipt",
            ),
            (
                len(inventory.event_occurrences),
                event_occurrences,
                MAX_EVENT_OCCURRENCES,
                "event occurrence",
            ),
            (
                len(inventory.source_rotations),
                source_rotations,
                MAX_SOURCE_ROTATIONS,
                "source rotation",
            ),
        )
        for existing, additions, cap, label in prospective:
            if type(additions) is not int or additions < 0:
                raise TypeError(f"{label} additions must be a nonnegative int")
            if existing + additions > cap:
                raise JournalIntegrityError(f"{label} inventory exceeds its cap")

    def _snapshot_diagnostic_rows(
        self,
    ) -> tuple[DiagnosticIngestionScope | None, DiagnosticInventory]:
        with self._diagnostic_read():
            owner = self._diagnostic_owner()
            return (
                self._load_diagnostic_scope(owner),
                self._load_diagnostic_inventory(owner),
            )
