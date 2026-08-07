"""Public vocabulary for the state of one room's continuity recovery.

An application that owns its own durable Classic Sync checkpoint has to
distinguish a recovery walk that is still making headway from one that is
wedged, and both of those from history nio has already given up on. A single
"unrecovered" flag cannot carry that distinction, and inferring it from
``timeline.limited``, token shapes, or pagination shapes is exactly the guess
that produces silent history loss.
"""

from dataclasses import dataclass
from enum import Enum


@dataclass(frozen=True)
class AbandonedRecovery:
    """One room's continuity debt, given up on.

    ``unwalked_from_token`` and ``unwalked_to_token`` bound the span the walk
    never covered, in the server's own token space.
    ``unwalked_event_count`` is ``None`` when the walk never reached its
    target: nio never fetched that span and cannot say how much is in it. It is
    ``0`` when the walk had already finished and only undelivered callbacks
    remained, where the whole loss is ``discarded_recovered_events``.

    This lives beside :class:`RoomRecoveryStatus` rather than with the recovery
    machinery because a response carries it, and ``responses`` is imported by
    that machinery rather than the other way round.
    """

    room_id: str
    unwalked_from_token: str | None
    unwalked_to_token: str
    unwalked_event_count: int | None
    discarded_recovered_events: int
    retained_live_events: int


class RoomRecoveryStatus(str, Enum):
    """What this response settled about one room's timeline continuity.

    ``RECOVERED`` means the room's gap closed and every recovered event was
    dispatched. ``CONVERGING`` and ``STALLED`` both mean the gap is still owed:
    the walk advanced this pump, or it did not. ``LOST`` means nio dropped
    history for this room that it will never deliver, so no amount of waiting
    will complete it.

    ``LOST`` takes precedence over an owed gap for the same room in the same
    response: the loss is the irreversible fact, and the gap is reported again
    on the next response while it remains outstanding.
    """

    RECOVERED = "recovered"
    CONVERGING = "converging"
    STALLED = "stalled"
    LOST = "lost"
