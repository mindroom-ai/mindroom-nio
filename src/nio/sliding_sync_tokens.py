"""Membership-scoped recovery cursors for Simplified Sliding Sync."""

from dataclasses import dataclass


@dataclass(frozen=True)
class SlidingWindowToken:
    """A room-history cursor bound to one exact own-membership event.

    Attributes:
        token: The room's ``prev_batch`` cursor used to recover an omitted
            timeline interval.
        membership_event_id: The event ID proving the user's current joined
            membership. The token must not be reused after that membership
            ends or changes without an exact linked join-to-join replacement.
    """

    token: str
    membership_event_id: str

    def __post_init__(self) -> None:
        if not self.membership_event_id:
            raise ValueError("membership_event_id must be a non-empty string")
