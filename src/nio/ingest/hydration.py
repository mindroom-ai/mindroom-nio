"""Private, intent-bound room-state hydration values."""

from dataclasses import dataclass, fields  # noqa: I001
from typing import Any, cast

from ._json import canonical_json, load_json
from .model import RecordOrigin
from .ports import MAX_CANONICAL_STAGED_RESPONSE_BODY_BYTES as _BODY_LIMIT
from .reducer import HydrationIntent, RoomContinuity

# fmt: off
@dataclass(frozen=True, slots=True)  # noqa: E302
class PendingHydration:
    continuity: RoomContinuity
    intent: HydrationIntent

    def __post_init__(self) -> None:
        if (type(self.continuity), type(self.intent)) != (RoomContinuity, HydrationIntent):
            raise TypeError("invalid pending hydration types")
        state = self.continuity
        identity = (state.membership, state.baseline, state.gap, state.hydration_id)
        if not state.room_id or state.membership_epoch < 0 or identity != ("join", None, None, self.intent.hydration_id):
            raise ValueError("pending hydration is not an exact JOIN barrier")
        origin = self.intent.origin
        if min(origin.source_epoch, origin.request_id, origin.frame_index) < 0:
            raise ValueError("hydration origin must be nonnegative")


@dataclass(frozen=True, slots=True)
class HydrationResult:
    pending: PendingHydration
    response_body: bytes

    def __post_init__(self) -> None:
        if type(self.pending) is not PendingHydration or type(self.response_body) is not bytes:
            raise TypeError("invalid hydration result types")
        if len(self.response_body) > _BODY_LIMIT:
            raise ValueError("response_body exceeds 16 MiB")
        value = load_json(self.response_body, "hydration response")
        if type(value) is not list or canonical_json(value) != self.response_body:
            raise ValueError("hydration response must be a canonical JSON array")


def _verified_body(pending: PendingHydration, own_user_id: str, body: bytes) -> tuple[bytes, str]:
    if type(own_user_id) is not str or not own_user_id or type(body) is not bytes:
        raise TypeError("invalid hydration response arguments")
    if len(body) > _BODY_LIMIT:
        raise ValueError("hydration response exceeds 16 MiB")
    value = load_json(body, "hydration response")
    if type(value) is not list:
        raise ValueError("hydration response must be a JSON array")
    seen: set[tuple[str, str]] = set()
    membership_id: str | None = None
    for event in value:
        if type(event) is not dict:
            raise ValueError("hydration state event must be an object")
        event_type, state_key = event.get("type"), event.get("state_key")
        content, event_id = event.get("content"), event.get("event_id")
        field_types = (type(event_type), type(state_key), type(content), type(event_id))
        if field_types != (str, str, dict, str) or not event_type or not event_id:
            raise ValueError("hydration state event has invalid required fields")
        event_type = cast("str", event_type)
        state_key = cast("str", state_key)
        content = cast("dict[str, Any]", content)
        event_id = cast("str", event_id)
        key = (event_type, state_key)
        if key in seen or event_type == "m.room.encrypted":
            raise ValueError("duplicate or encrypted hydration state")
        seen.add(key)
        if key == ("m.room.member", own_user_id):
            if content.get("membership") != pending.continuity.membership:
                raise ValueError("own membership contradicts pending hydration")
            membership_id = event_id
    if membership_id is None:
        raise ValueError("hydration requires exactly one own membership event")
    return canonical_json(value), membership_id


def normalize_hydration_response(pending: PendingHydration, *, own_user_id: str, response_body: bytes) -> HydrationResult:
    if type(pending) is not PendingHydration:
        raise TypeError("pending must be PendingHydration")
    canonical, _ = _verified_body(pending, own_user_id, response_body)
    return HydrationResult(pending, canonical)


def revalidated_hydration_result(result: HydrationResult, *, own_user_id: str) -> tuple[HydrationResult, str]:
    if type(result) is not HydrationResult:
        raise TypeError("result must be HydrationResult")
    state, intent = result.pending.continuity, result.pending.intent
    origin = intent.origin
    origin = RecordOrigin(*(getattr(origin, field.name) for field in fields(origin)))
    state = RoomContinuity(*(getattr(state, field.name) for field in fields(state)))
    pending = PendingHydration(state, HydrationIntent(intent.hydration_id, origin))
    rebuilt = HydrationResult(pending, result.response_body)
    canonical, event_id = _verified_body(pending, own_user_id, rebuilt.response_body)
    if canonical != rebuilt.response_body:
        raise ValueError("hydration response is not canonical")
    return rebuilt, event_id
