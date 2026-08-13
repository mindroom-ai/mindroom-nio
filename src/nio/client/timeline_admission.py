"""Public types for ordered timeline batch admission."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import Enum

from ..event_provenance import TimelineEventProvenance
from ..events import Event
from ..rooms import MatrixRoom


@dataclass(frozen=True)
class TimelineAdmissionEntry:
    """One prepared timeline event presented to the admission owner."""

    room: MatrixRoom
    event: Event
    provenance: TimelineEventProvenance


class TimelineAdmissionDisposition(str, Enum):
    """The admission owner's semantic classification for one entry."""

    FANOUT = "fanout"
    NON_SEMANTIC = "non_semantic"


TimelineBatchAdmissionCallback = Callable[
    [tuple[TimelineAdmissionEntry, ...]],
    Awaitable[tuple[TimelineAdmissionDisposition, ...]]
    | tuple[TimelineAdmissionDisposition, ...],
]


@dataclass(frozen=True)
class _TimelineBatchAdmission:
    callback: TimelineBatchAdmissionCallback
    event_filter: type[Event] | tuple[type[Event], ...] | None
