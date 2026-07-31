from dataclasses import dataclass


@dataclass(frozen=True)
class SlidingWindowToken:
    token: str
    membership_event_id: str

    def __post_init__(self) -> None:
        if not self.membership_event_id:
            raise ValueError("membership_event_id must be a non-empty string")
