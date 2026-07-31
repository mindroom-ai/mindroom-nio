from enum import Enum


class TimelineEventProvenance(str, Enum):
    """Whether a timeline event originated from live sync or history."""

    LIVE = "live"
    HISTORY = "history"
