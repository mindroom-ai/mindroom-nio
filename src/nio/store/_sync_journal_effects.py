"""Authenticated persistence helpers for durable ingestion network effects."""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass, replace
from typing import cast
from uuid import UUID

from ..ingest._json import load_internal_json
from ..ingest.effects import (
    MembershipAction,
    MembershipDeliveryState,
    MembershipRequest,
    NetworkEffectKind,
    NetworkEffectRequest,
    PersistedNetworkEffect,
    RecoveryRequest,
    RoomHydrationRequest,
)
from ..ingest.errors import JournalIntegrityError
from ..ingest.model import RoomHydrationStatus, TransportKind
from ..ingest.serialization import (
    _canonical_json,
    _decoded_bytes,
    _encoded_bytes,
)
from ..ingest.state import OwnerView, RoomAggregate

_NETWORK_EFFECT_COMMON_FIELDS = (
    "effect_id",
    "effect_kind",
    "stream_id",
    "transport",
    "room_id",
    "membership_epoch",
)
_RECOVERY_REQUEST_FIELDS = (
    *_NETWORK_EFFECT_COMMON_FIELDS,
    "gap_id",
    "page_ordinal",
    "from_token",
    "to_token",
    "limit",
    "timeout_ms",
)
_ROOM_HYDRATION_REQUEST_FIELDS = (
    *_NETWORK_EFFECT_COMMON_FIELDS,
    "timeout_ms",
)
_MEMBERSHIP_REQUEST_FIELDS = (
    *_NETWORK_EFFECT_COMMON_FIELDS,
    "action",
    "request_body",
    "timeout_ms",
)
_NETWORK_EFFECT_STATE_FIELDS = (
    "effect_id",
    "effect_kind",
    "attempt_ordinal",
    "membership_delivery_state",
    "prior_delivery_uncertain",
    "created_revision",
    "updated_revision",
)
_NETWORK_EFFECT_SQL_CHUNK_SIZE = 32


@dataclass(frozen=True, slots=True)
class _StoredNetworkEffect:
    effect: PersistedNetworkEffect
    created_revision: int
    updated_revision: int
    request_sha256: bytes
    state_sha256: bytes


@dataclass(frozen=True, slots=True)
class _NetworkEffectStateUpdate:
    effect: PersistedNetworkEffect
    created_revision: int
    previous_updated_revision: int
    request_sha256: bytes
    previous_state_sha256: bytes
    state_ciphertext: bytes
    state_sha256: bytes


class NetworkEffectRows:
    """Mixin requiring the journal's codec, connection, and raw room loader."""

    @staticmethod
    def _network_effect_kind(request: NetworkEffectRequest) -> NetworkEffectKind:
        if type(request) is RecoveryRequest:
            return NetworkEffectKind.RECOVERY
        if type(request) is RoomHydrationRequest:
            return NetworkEffectKind.ROOM_HYDRATION
        if type(request) is MembershipRequest:
            return NetworkEffectKind.MEMBERSHIP
        raise TypeError("request must be a NetworkEffectRequest")

    @classmethod
    def _network_effect_request_payload(
        cls,
        request: NetworkEffectRequest,
    ) -> bytes:
        kind = cls._network_effect_kind(request)
        payload: dict[str, object] = {
            "effect_id": str(request.effect_id),
            "effect_kind": kind.value,
            "stream_id": str(request.stream_id),
            "transport": request.transport.value,
            "room_id": request.room_id,
            "membership_epoch": request.membership_epoch,
        }
        if type(request) is RecoveryRequest:
            payload.update(
                {
                    "gap_id": str(request.gap_id),
                    "page_ordinal": request.page_ordinal,
                    "from_token": request.from_token,
                    "to_token": request.to_token,
                    "limit": request.limit,
                    "timeout_ms": request.timeout_ms,
                }
            )
        elif type(request) is RoomHydrationRequest:
            payload["timeout_ms"] = request.timeout_ms
        else:
            assert type(request) is MembershipRequest
            payload.update(
                {
                    "action": request.action.value,
                    "request_body": _encoded_bytes(request.request_body),
                    "timeout_ms": request.timeout_ms,
                }
            )
        return _canonical_json(payload)

    @classmethod
    def _network_effect_state_payload(
        cls,
        effect: PersistedNetworkEffect,
        created_revision: int,
        updated_revision: int,
    ) -> bytes:
        kind = cls._network_effect_kind(effect.request)
        return _canonical_json(
            {
                "effect_id": str(effect.request.effect_id),
                "effect_kind": kind.value,
                "attempt_ordinal": effect.attempt_ordinal,
                "membership_delivery_state": (
                    effect.membership_delivery_state.value
                    if effect.membership_delivery_state is not None
                    else None
                ),
                "prior_delivery_uncertain": effect.prior_delivery_uncertain,
                "created_revision": created_revision,
                "updated_revision": updated_revision,
            }
        )

    def _validate_network_effect(
        self,
        effect: PersistedNetworkEffect,
        owner: OwnerView,
    ) -> NetworkEffectKind:
        try:
            request = replace(effect.request)
            replace(effect, request=request)
            kind = self._network_effect_kind(request)
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError("network effect is invalid") from error
        if (
            request.stream_id != owner.stream_id
            or request.transport is not owner.transport_kind
        ):
            raise JournalIntegrityError(
                "network effect stream or transport does not match journal owner"
            )
        return kind

    @staticmethod
    def _network_effect_request_from_envelope(
        envelope: dict[str, object],
    ) -> NetworkEffectRequest:
        kind = NetworkEffectKind(envelope["effect_kind"])
        expected_fields = {
            NetworkEffectKind.RECOVERY: _RECOVERY_REQUEST_FIELDS,
            NetworkEffectKind.ROOM_HYDRATION: _ROOM_HYDRATION_REQUEST_FIELDS,
            NetworkEffectKind.MEMBERSHIP: _MEMBERSHIP_REQUEST_FIELDS,
        }[kind]
        if tuple(envelope) != expected_fields:
            raise ValueError("network effect request fields are not canonical")
        common = (
            UUID(envelope["effect_id"]),
            UUID(envelope["stream_id"]),
            TransportKind(envelope["transport"]),
            envelope["room_id"],
            envelope["membership_epoch"],
        )
        if kind is NetworkEffectKind.RECOVERY:
            return RecoveryRequest(
                *common,
                UUID(envelope["gap_id"]),
                envelope["page_ordinal"],
                envelope["from_token"],
                envelope["to_token"],
                envelope["limit"],
                envelope["timeout_ms"],
            )
        if kind is NetworkEffectKind.ROOM_HYDRATION:
            return RoomHydrationRequest(*common, envelope["timeout_ms"])
        request_body = _decoded_bytes(
            envelope["request_body"],
            "membership request body",
        )
        if request_body is None:
            raise ValueError("membership request body must not be null")
        return MembershipRequest(
            *common,
            MembershipAction(envelope["action"]),
            request_body,
            envelope["timeout_ms"],
        )

    def _decode_network_effect_row(
        self,
        row: sqlite3.Row,
        owner: OwnerView,
    ) -> _StoredNetworkEffect:
        try:
            effect_id = UUID(row["effect_id"])
            primary_key = (effect_id,)
            request_payload = self._open_payload(
                "NioIngestNetworkEffect.request",
                primary_key,
                row,
                "request",
            )
            request_value = load_internal_json(
                request_payload,
                "network effect request envelope",
            )
            if type(request_value) is not dict:
                raise ValueError("network effect request envelope must be an object")
            request_envelope = cast(dict[str, object], request_value)
            request = self._network_effect_request_from_envelope(request_envelope)
            state_payload = self._open_payload(
                "NioIngestNetworkEffect.state",
                primary_key,
                row,
                "state",
            )
            state_value = load_internal_json(
                state_payload,
                "network effect state envelope",
            )
            if type(state_value) is not dict:
                raise ValueError("network effect state envelope must be an object")
            state_envelope = cast(dict[str, object], state_value)
            if tuple(state_envelope) != _NETWORK_EFFECT_STATE_FIELDS:
                raise ValueError("network effect state fields are not canonical")
            delivery_value = state_envelope["membership_delivery_state"]
            delivery = (
                MembershipDeliveryState(delivery_value)
                if delivery_value is not None
                else None
            )
            effect = PersistedNetworkEffect(
                request,
                state_envelope["attempt_ordinal"],
                delivery,
                state_envelope["prior_delivery_uncertain"],
            )
            created_revision = state_envelope["created_revision"]
            updated_revision = state_envelope["updated_revision"]
            if (
                type(created_revision) is not int
                or type(updated_revision) is not int
                or created_revision < 0
                or updated_revision < created_revision
                or updated_revision > owner.revision
            ):
                raise ValueError("network effect revisions are invalid")
            kind = self._validate_network_effect(effect, owner)
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise JournalIntegrityError(
                "network effect authenticated envelope is invalid"
            ) from error

        try:
            canonical_request = self._network_effect_request_payload(request)
            canonical_state = self._network_effect_state_payload(
                effect,
                created_revision,
                updated_revision,
            )
        except (TypeError, UnicodeEncodeError, ValueError) as error:
            raise JournalIntegrityError(
                "network effect authenticated envelope is invalid"
            ) from error
        if request_payload != canonical_request:
            raise JournalIntegrityError(
                "network effect request envelope is not canonical"
            )
        if state_payload != canonical_state:
            raise JournalIntegrityError(
                "network effect state envelope is not canonical"
            )
        if (
            state_envelope["effect_id"] != str(request.effect_id)
            or state_envelope["effect_kind"] != kind.value
        ):
            raise JournalIntegrityError(
                "network effect request and state identities do not match"
            )

        columns = tuple(
            row[name]
            for name in (
                "effect_id",
                "effect_kind",
                "room_id",
                "membership_epoch",
                "attempt_ordinal",
                "membership_delivery_state",
                "prior_delivery_uncertain",
                "created_revision",
                "updated_revision",
            )
        )
        for value, expected in zip(
            columns,
            (str, str, str, int, int, object, object, int, int),
            strict=True,
        ):
            if expected is not object and type(value) is not expected:
                raise JournalIntegrityError("network effect columns have invalid types")
        if (
            row["membership_delivery_state"] is not None
            and type(row["membership_delivery_state"]) is not str
        ):
            raise JournalIntegrityError("network effect delivery column is invalid")
        if (
            row["prior_delivery_uncertain"] is not None
            and type(row["prior_delivery_uncertain"]) is not int
        ):
            raise JournalIntegrityError("network effect uncertainty column is invalid")
        authenticated = (
            str(request.effect_id),
            kind.value,
            request.room_id,
            request.membership_epoch,
            effect.attempt_ordinal,
            (
                effect.membership_delivery_state.value
                if effect.membership_delivery_state is not None
                else None
            ),
            (
                int(effect.prior_delivery_uncertain)
                if effect.prior_delivery_uncertain is not None
                else None
            ),
            created_revision,
            updated_revision,
        )
        if columns != authenticated:
            raise JournalIntegrityError(
                "network effect columns do not match authenticated envelopes"
            )
        request_sha256 = bytes(row["request_sha256"])
        state_sha256 = bytes(row["state_sha256"])
        return _StoredNetworkEffect(
            effect,
            created_revision,
            updated_revision,
            request_sha256,
            state_sha256,
        )

    @staticmethod
    def _validate_network_effect_room_link(
        effect: PersistedNetworkEffect,
        aggregate: RoomAggregate | None,
        *,
        insertion: bool,
    ) -> None:
        request = effect.request
        if (
            aggregate is None
            or aggregate.state.room_id != request.room_id
            or aggregate.state.current_membership_epoch != request.membership_epoch
            or aggregate.active_lane.membership_epoch != request.membership_epoch
        ):
            raise JournalIntegrityError(
                "network effect does not match the current room epoch"
            )
        if type(request) is RoomHydrationRequest:
            if aggregate.state.hydration_status is not RoomHydrationStatus.PENDING:
                raise JournalIntegrityError(
                    "room hydration effect requires a pending room"
                )
            return
        if type(request) is MembershipRequest:
            return
        if type(request) is not RecoveryRequest:
            raise JournalIntegrityError("network effect request kind is invalid")
        gap = aggregate.active_lane.recovery_gap
        if (
            gap is None
            or gap.in_flight_effect_id != request.effect_id
            or gap.gap_id != request.gap_id
            or gap.pages_committed != request.page_ordinal
            or gap.cursor_token != request.from_token
            or (insertion and gap.target_token != request.to_token)
        ):
            raise JournalIntegrityError(
                "recovery effect does not match the active recovery gap"
            )

    def _validate_network_effect_graph(
        self,
        aggregates: dict[str, RoomAggregate],
        effects: dict[UUID, PersistedNetworkEffect],
        *,
        insertion_ids: frozenset[UUID] = frozenset(),
    ) -> None:
        memberships_by_room: dict[str, UUID] = {}
        for effect_id, effect in effects.items():
            request = effect.request
            self._validate_network_effect_room_link(
                effect,
                aggregates.get(request.room_id),
                insertion=effect_id in insertion_ids,
            )
            if type(request) is MembershipRequest:
                previous = memberships_by_room.setdefault(request.room_id, effect_id)
                if previous != effect_id:
                    raise JournalIntegrityError(
                        "room has more than one unresolved membership effect"
                    )

        for aggregate in aggregates.values():
            gap = aggregate.active_lane.recovery_gap
            if gap is None or gap.in_flight_effect_id is None:
                continue
            linked = effects.get(gap.in_flight_effect_id)
            if linked is None or type(linked.request) is not RecoveryRequest:
                raise JournalIntegrityError(
                    "recovery gap pointer has no matching recovery effect"
                )
            self._validate_network_effect_room_link(
                linked,
                aggregate,
                insertion=gap.in_flight_effect_id in insertion_ids,
            )

    def _load_network_effect_row(
        self,
        effect_id: UUID,
        owner: OwnerView,
    ) -> _StoredNetworkEffect | None:
        row = self.connection.execute(
            "SELECT * FROM NioIngestNetworkEffect "
            "WHERE account_id = ? AND effect_id = ?",
            (self.account_id, str(effect_id)),
        ).fetchone()
        return self._decode_network_effect_row(row, owner) if row is not None else None

    def _load_network_effect_rows_by_ids(
        self,
        effect_ids: tuple[UUID, ...],
        owner: OwnerView,
    ) -> dict[UUID, _StoredNetworkEffect]:
        decoded: dict[UUID, _StoredNetworkEffect] = {}
        for offset in range(0, len(effect_ids), _NETWORK_EFFECT_SQL_CHUNK_SIZE):
            chunk = effect_ids[offset : offset + _NETWORK_EFFECT_SQL_CHUNK_SIZE]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.connection.execute(
                "SELECT * FROM NioIngestNetworkEffect WHERE account_id = ? "
                f"AND effect_id IN ({placeholders})",
                (self.account_id, *(str(effect_id) for effect_id in chunk)),
            ).fetchall()
            for row in rows:
                value = self._decode_network_effect_row(row, owner)
                decoded[value.effect.request.effect_id] = value
        return decoded

    def _load_network_effect_rows_for_rooms(
        self,
        room_ids: frozenset[str],
        owner: OwnerView,
    ) -> tuple[_StoredNetworkEffect, ...]:
        if not room_ids:
            return ()
        ordered_ids = tuple(sorted(room_ids))
        decoded = []
        for offset in range(0, len(ordered_ids), _NETWORK_EFFECT_SQL_CHUNK_SIZE):
            chunk = ordered_ids[offset : offset + _NETWORK_EFFECT_SQL_CHUNK_SIZE]
            placeholders = ",".join("?" for _ in chunk)
            rows = self.connection.execute(
                "SELECT * FROM NioIngestNetworkEffect WHERE account_id = ? "
                f"AND room_id IN ({placeholders}) "
                "ORDER BY created_revision, effect_id",
                (self.account_id, *chunk),
            ).fetchall()
            decoded.extend(self._decode_network_effect_row(row, owner) for row in rows)
        return tuple(
            sorted(
                decoded,
                key=lambda value: (
                    value.created_revision,
                    str(value.effect.request.effect_id),
                ),
            )
        )

    def _validate_loaded_network_effects(
        self,
        decoded: tuple[_StoredNetworkEffect, ...],
    ) -> tuple[PersistedNetworkEffect, ...]:
        effects = tuple(value.effect for value in decoded)
        room_ids = frozenset(effect.request.room_id for effect in effects)
        aggregates = self._load_room_aggregates(room_ids) if room_ids else {}
        graph_effects = {effect.request.effect_id: effect for effect in effects}
        missing_pointer_ids = tuple(
            gap.in_flight_effect_id
            for aggregate in aggregates.values()
            if (gap := aggregate.active_lane.recovery_gap) is not None
            and gap.in_flight_effect_id is not None
            and gap.in_flight_effect_id not in graph_effects
        )
        if missing_pointer_ids:
            owner = self._require_attached()
            graph_effects.update(
                {
                    effect_id: stored.effect
                    for effect_id, stored in self._load_network_effect_rows_by_ids(
                        missing_pointer_ids,
                        owner,
                    ).items()
                }
            )
        self._validate_network_effect_graph(
            aggregates,
            graph_effects,
        )
        return effects

    def load_network_effect(
        self,
        effect_id: UUID,
    ) -> PersistedNetworkEffect | None:
        owner = self._require_attached()
        if type(effect_id) is not UUID:
            raise TypeError("effect_id must be UUID")
        decoded = self._load_network_effect_row(effect_id, owner)
        if decoded is None:
            return None
        return self._validate_loaded_network_effects((decoded,))[0]

    @staticmethod
    def _validate_network_effect_list_limit(limit: int) -> None:
        if type(limit) is not int:
            raise TypeError("limit must be int")
        if not 1 <= limit <= 256:
            raise ValueError("limit must be between 1 and 256")

    def _list_network_effects(
        self,
        limit: int,
        *,
        schedulable_only: bool,
    ) -> tuple[PersistedNetworkEffect, ...]:
        owner = self._require_attached()
        self._validate_network_effect_list_limit(limit)
        predicate = (
            " AND (effect_kind != 'membership' "
            "OR membership_delivery_state = 'ready')"
            if schedulable_only
            else ""
        )
        rows = self.connection.execute(
            "SELECT * FROM NioIngestNetworkEffect WHERE account_id = ?"
            f"{predicate} ORDER BY created_revision, effect_id LIMIT ?",
            (self.account_id, limit),
        ).fetchall()
        return self._validate_loaded_network_effects(
            tuple(self._decode_network_effect_row(row, owner) for row in rows)
        )

    def list_network_effects(
        self,
        limit: int,
    ) -> tuple[PersistedNetworkEffect, ...]:
        return self._list_network_effects(limit, schedulable_only=False)

    def list_schedulable_network_effects(
        self,
        limit: int,
    ) -> tuple[PersistedNetworkEffect, ...]:
        return self._list_network_effects(limit, schedulable_only=True)

    def _network_effect_insert_values(
        self,
        effect: PersistedNetworkEffect,
        revision: int,
        owner: OwnerView,
    ) -> tuple[object, ...]:
        kind = self._validate_network_effect(effect, owner)
        request = effect.request
        primary_key = (request.effect_id,)
        request_ciphertext, request_digest = self._codec.seal(
            "NioIngestNetworkEffect.request",
            primary_key,
            self._network_effect_request_payload(request),
        )
        state_ciphertext, state_digest = self._codec.seal(
            "NioIngestNetworkEffect.state",
            primary_key,
            self._network_effect_state_payload(effect, revision, revision),
        )
        return (
            self.account_id,
            str(request.effect_id),
            kind.value,
            request.room_id,
            request.membership_epoch,
            effect.attempt_ordinal,
            (
                effect.membership_delivery_state.value
                if effect.membership_delivery_state is not None
                else None
            ),
            (
                int(effect.prior_delivery_uncertain)
                if effect.prior_delivery_uncertain is not None
                else None
            ),
            request_ciphertext,
            request_digest,
            state_ciphertext,
            state_digest,
            revision,
            revision,
        )

    @staticmethod
    def _validate_network_effect_update_edge(
        stored: PersistedNetworkEffect,
        proposed: PersistedNetworkEffect,
    ) -> bool:
        if stored.request != proposed.request:
            raise JournalIntegrityError(
                "network effect update cannot rewrite its immutable request"
            )
        if stored == proposed:
            return False
        if type(stored.request) is not MembershipRequest:
            raise JournalIntegrityError(
                "recovery and hydration effect state is immutable"
            )
        if stored.prior_delivery_uncertain != proposed.prior_delivery_uncertain:
            raise JournalIntegrityError(
                "generic membership update cannot change prior uncertainty"
            )
        if (
            stored.membership_delivery_state is MembershipDeliveryState.READY
            and proposed.membership_delivery_state
            is MembershipDeliveryState.DISPATCHED_UNCONFIRMED
            and proposed.attempt_ordinal == stored.attempt_ordinal + 1
        ):
            return True
        if (
            stored.membership_delivery_state
            is MembershipDeliveryState.DISPATCHED_UNCONFIRMED
            and stored.prior_delivery_uncertain is False
            and proposed.membership_delivery_state is MembershipDeliveryState.READY
            and proposed.attempt_ordinal == stored.attempt_ordinal
        ):
            return True
        raise JournalIntegrityError("membership effect state transition is invalid")

    def _prepare_network_effect_state_update(
        self,
        stored: _StoredNetworkEffect,
        effect: PersistedNetworkEffect,
        revision: int,
    ) -> _NetworkEffectStateUpdate:
        state_payload = self._network_effect_state_payload(
            effect,
            stored.created_revision,
            revision,
        )
        state_ciphertext, state_sha256 = self._codec.seal(
            "NioIngestNetworkEffect.state",
            (effect.request.effect_id,),
            state_payload,
        )
        return _NetworkEffectStateUpdate(
            effect,
            stored.created_revision,
            stored.updated_revision,
            stored.request_sha256,
            stored.state_sha256,
            state_ciphertext,
            state_sha256,
        )

    def _prepare_network_effect_state_updates(
        self,
        changes: tuple[tuple[_StoredNetworkEffect, PersistedNetworkEffect], ...],
        revision: int,
    ) -> tuple[_NetworkEffectStateUpdate, ...]:
        try:
            return tuple(
                self._prepare_network_effect_state_update(stored, effect, revision)
                for stored, effect in changes
            )
        except (
            AttributeError,
            KeyError,
            TypeError,
            UnicodeEncodeError,
            ValueError,
        ) as error:
            raise JournalIntegrityError(
                "network effect state envelope preparation failed"
            ) from error

    def _update_network_effects(
        self,
        changes: tuple[_NetworkEffectStateUpdate, ...],
        revision: int,
    ) -> None:
        for offset in range(0, len(changes), _NETWORK_EFFECT_SQL_CHUNK_SIZE):
            chunk = changes[offset : offset + _NETWORK_EFFECT_SQL_CHUNK_SIZE]
            assignments = (
                (
                    "attempt_ordinal",
                    tuple(change.effect.attempt_ordinal for change in chunk),
                ),
                (
                    "membership_delivery_state",
                    tuple(
                        (
                            change.effect.membership_delivery_state.value
                            if change.effect.membership_delivery_state is not None
                            else None
                        )
                        for change in chunk
                    ),
                ),
                (
                    "prior_delivery_uncertain",
                    tuple(
                        (
                            int(change.effect.prior_delivery_uncertain)
                            if change.effect.prior_delivery_uncertain is not None
                            else None
                        )
                        for change in chunk
                    ),
                ),
                (
                    "state_ciphertext",
                    tuple(change.state_ciphertext for change in chunk),
                ),
                ("state_sha256", tuple(change.state_sha256 for change in chunk)),
                ("updated_revision", tuple(revision for _ in chunk)),
            )
            set_clauses = []
            parameters: list[object] = []
            for column, values in assignments:
                set_clauses.append(
                    f"{column} = CASE effect_id "
                    + " ".join("WHEN ? THEN ?" for _ in chunk)
                    + f" ELSE {column} END"
                )
                for change, value in zip(chunk, values, strict=True):
                    parameters.extend((str(change.effect.request.effect_id), value))
            conditions = []
            parameters.append(self.account_id)
            for change in chunk:
                conditions.append(
                    "(effect_id = ? AND request_sha256 = ? "
                    "AND state_sha256 = ? AND updated_revision = ?)"
                )
                parameters.extend(
                    (
                        str(change.effect.request.effect_id),
                        change.request_sha256,
                        change.previous_state_sha256,
                        change.previous_updated_revision,
                    )
                )
            cursor = self._transition_execute(
                "network_effect_update",
                "UPDATE NioIngestNetworkEffect SET "
                + ", ".join(set_clauses)
                + " WHERE account_id = ? AND ("
                + " OR ".join(conditions)
                + ")",
                tuple(parameters),
            )
            if cursor.rowcount != len(chunk):
                raise JournalIntegrityError(
                    "network effect state update compare-and-swap failed"
                )

    def _delete_network_effects(
        self,
        stored_effects: tuple[_StoredNetworkEffect, ...],
    ) -> None:
        for offset in range(
            0,
            len(stored_effects),
            _NETWORK_EFFECT_SQL_CHUNK_SIZE,
        ):
            chunk = stored_effects[offset : offset + _NETWORK_EFFECT_SQL_CHUNK_SIZE]
            conditions = []
            parameters: list[object] = [self.account_id]
            for stored in chunk:
                conditions.append(
                    "(effect_id = ? AND request_sha256 = ? "
                    "AND state_sha256 = ? AND updated_revision = ?)"
                )
                parameters.extend(
                    (
                        str(stored.effect.request.effect_id),
                        stored.request_sha256,
                        stored.state_sha256,
                        stored.updated_revision,
                    )
                )
            cursor = self._transition_execute(
                "network_effect_delete",
                "DELETE FROM NioIngestNetworkEffect WHERE account_id = ? AND ("
                + " OR ".join(conditions)
                + ")",
                tuple(parameters),
            )
            if cursor.rowcount != len(chunk):
                raise JournalIntegrityError(
                    "network effect delete compare-and-swap failed"
                )

    def _prepare_network_effect_inserts(
        self,
        effects: tuple[PersistedNetworkEffect, ...],
        revision: int,
        owner: OwnerView,
    ) -> tuple[tuple[object, ...], ...]:
        try:
            return tuple(
                self._network_effect_insert_values(effect, revision, owner)
                for effect in effects
            )
        except (
            AttributeError,
            KeyError,
            TypeError,
            UnicodeEncodeError,
            ValueError,
        ) as error:
            raise JournalIntegrityError(
                "network effect request or state envelope preparation failed"
            ) from error

    def _insert_network_effects(
        self,
        values: tuple[tuple[object, ...], ...],
    ) -> None:
        for offset in range(0, len(values), _NETWORK_EFFECT_SQL_CHUNK_SIZE):
            chunk = values[offset : offset + _NETWORK_EFFECT_SQL_CHUNK_SIZE]
            placeholders = ",".join(
                "(" + ",".join("?" for _ in row) + ")" for row in chunk
            )
            parameters = tuple(item for row in chunk for item in row)
            cursor = self._transition_execute(
                "network_effect_insert",
                "INSERT INTO NioIngestNetworkEffect ("
                "account_id, effect_id, effect_kind, room_id, membership_epoch, "
                "attempt_ordinal, membership_delivery_state, "
                "prior_delivery_uncertain, request_ciphertext, request_sha256, "
                "state_ciphertext, state_sha256, created_revision, updated_revision"
                f") VALUES {placeholders}",
                parameters,
            )
            if cursor.rowcount != len(chunk):
                raise JournalIntegrityError("network effect insert group is incomplete")
