from enum import Enum


class TimelineEventProvenance(str, Enum):
    """Whether an event is live, continuity-recovered, or cold history."""

    LIVE = "live"
    RECOVERED = "recovered"
    HISTORY = "history"
