from enum import Enum


class TimelineEventProvenance(str, Enum):
    """Whether a timeline event is live activity or historical context."""

    LIVE = "live"
    HISTORY = "history"
