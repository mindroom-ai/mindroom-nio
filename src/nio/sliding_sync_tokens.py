from dataclasses import dataclass


@dataclass(frozen=True)
class SlidingWindowToken:
    token: str
    membership_event_id: str
