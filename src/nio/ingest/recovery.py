from dataclasses import dataclass
from uuid import UUID

from .model import RecordOrigin


def _require_exact(value: object, expected: type, field_name: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{field_name} must be {expected.__name__}")


def _require_nonempty_string(value: object, field_name: str) -> None:
    _require_exact(value, str, field_name)
    if not value:
        raise ValueError(f"{field_name} must not be empty")


def _require_nonnegative(value: object, field_name: str) -> None:
    _require_exact(value, int, field_name)
    if value < 0:
        raise ValueError(f"{field_name} must be nonnegative")


@dataclass(frozen=True, slots=True)
class RecoveryGap:
    gap_id: UUID
    room_id: str
    opening_source_epoch: int
    membership_epoch: int
    origin: RecordOrigin
    membership_event_id: str
    start_token: str
    target_token: str
    cursor_token: str
    seen_cursor_tokens: tuple[str, ...]
    pages_committed: int
    recovered_record_count: int
    in_flight_effect_id: UUID | None

    def __post_init__(self) -> None:
        _require_exact(self.gap_id, UUID, "gap_id")
        _require_nonempty_string(self.room_id, "room_id")
        _require_nonnegative(self.opening_source_epoch, "opening_source_epoch")
        _require_nonnegative(self.membership_epoch, "membership_epoch")
        _require_exact(self.origin, RecordOrigin, "origin")
        _require_nonempty_string(self.membership_event_id, "membership_event_id")
        for name in ("start_token", "target_token", "cursor_token"):
            _require_nonempty_string(getattr(self, name), name)
        if type(self.seen_cursor_tokens) is not tuple:
            raise TypeError("seen_cursor_tokens must be a tuple")
        if not self.seen_cursor_tokens:
            raise ValueError("seen_cursor_tokens must not be empty")
        if any(type(token) is not str for token in self.seen_cursor_tokens):
            raise TypeError("seen_cursor_tokens must contain str values")
        if any(not token for token in self.seen_cursor_tokens):
            raise ValueError("seen_cursor_tokens must not contain empty values")
        if len(set(self.seen_cursor_tokens)) != len(self.seen_cursor_tokens):
            raise ValueError("seen_cursor_tokens must be unique")
        if self.seen_cursor_tokens[0] != self.start_token:
            raise ValueError("seen_cursor_tokens must begin with start_token")
        if self.seen_cursor_tokens[-1] != self.cursor_token:
            raise ValueError("seen_cursor_tokens must end with cursor_token")
        _require_nonnegative(self.pages_committed, "pages_committed")
        _require_nonnegative(self.recovered_record_count, "recovered_record_count")
        if self.pages_committed != len(self.seen_cursor_tokens) - 1:
            raise ValueError("pages_committed does not match cursor history")
        if self.in_flight_effect_id is not None:
            _require_exact(self.in_flight_effect_id, UUID, "in_flight_effect_id")
        if self.origin.source_epoch != self.opening_source_epoch:
            raise ValueError("origin source epoch must match opening_source_epoch")
