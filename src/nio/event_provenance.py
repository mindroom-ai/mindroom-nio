from enum import Enum


class TimelineEventProvenance(str, Enum):
    """How a timeline event entered the current client timeline."""

    LIVE = "live"
    RECOVERED = "recovered"
    HISTORY = "history"
