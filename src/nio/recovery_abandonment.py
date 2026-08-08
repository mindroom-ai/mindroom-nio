from enum import Enum


class RecoveryAbandonment(str, Enum):
    """Why nio stopped recovering a room's limited-timeline gap.

    Abandonment covers situations an application has to treat differently, and
    one that treats them alike gets some of them wrong. ``EVENT_LIMIT``,
    ``FETCH_FAILED``, ``BASELINE_LOST`` and ``CORRUPT_EVENT`` say only that
    *nio* stopped: the missing history was never shown to be gone, and another
    recovery strategy may still be able to fetch it. ``UNVERIFIABLE`` says the
    walk cannot be made to work at all, so those events will never arrive.
    ``UNKNOWN`` says nio cannot answer the question.

    Recording every abandonment as permanent marks rooms incompletable that are
    merely over budget; recording none as permanent throws away the one signal
    that says history is genuinely lost. The reason is published so the
    application does not have to guess which case it is in.
    """

    EVENT_LIMIT = "event_limit"
    """A bound nio set on itself was reached: too many events held for a room,
    or too many recovered for one gap. A budget, not a finding."""

    FETCH_FAILED = "fetch_failed"
    """The server refused a backfill request in a way nio does not retry.
    A later session with different credentials or permissions may succeed."""

    BASELINE_LOST = "baseline_lost"
    """The gap was dropped along with the room state it was anchored to -- a
    membership reset, a leave or forget, or a discontinuity with no usable
    window token. Nio can never resume *this* walk, but nothing was shown about
    the history itself, and an application may still fetch it from the new
    baseline."""

    CORRUPT_EVENT = "corrupt_event"
    """A retained event could not be decoded, so nio cannot deliver the exact
    recovered slice. This names a local data failure; it does not prove that an
    application or a fresh fetch cannot recover the history another way."""

    UNKNOWN = "unknown"
    """Nio cannot say why the walk was given up on. Recorded by rows that
    predate the reason being tracked, and by any producer that fails to name a
    cause. It claims nothing, which is the point: an application must not read
    it as permission to assume the history is reachable."""

    UNVERIFIABLE = "unverifiable"
    """Continuity cannot be proven: pagination stalled, or ran out before
    reaching the target. Retrying the same walk cannot help, so the missing
    events are permanently lost."""

    @property
    def rank(self) -> int:
        """How little an application may assume recovery is still possible.

        This is deliberately *not* a scale of how much nio claims to know.
        ``UNKNOWN`` asserts nothing at all, yet outranks three reasons that each
        assert something, because a reason is only ever replaced by one that
        permits the application to assume less.

        Abandonment is sticky, so a room can be abandoned again before the
        application settles the first loss. Merging by recency would let a
        proven-unreachable gap be relabelled as merely over budget, or an
        undiagnosed legacy loss be relabelled as a known-recoverable one --
        the same under-reporting stickiness exists to prevent, moved into the
        reason.
        """
        return _RANKS[self]


_RANKS = {
    RecoveryAbandonment.EVENT_LIMIT: 0,
    RecoveryAbandonment.FETCH_FAILED: 1,
    RecoveryAbandonment.BASELINE_LOST: 2,
    RecoveryAbandonment.CORRUPT_EVENT: 3,
    RecoveryAbandonment.UNKNOWN: 4,
    RecoveryAbandonment.UNVERIFIABLE: 5,
}


def most_conservative_abandonment(
    current: RecoveryAbandonment | None,
    incoming: RecoveryAbandonment,
) -> RecoveryAbandonment:
    """Keep whichever reason lets the application assume the least."""
    if current is None:
        return incoming
    return max(current, incoming, key=lambda reason: reason.rank)
