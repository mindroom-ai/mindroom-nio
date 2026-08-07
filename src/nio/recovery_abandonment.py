from enum import Enum


class RecoveryAbandonment(str, Enum):
    """Why nio stopped recovering a room's limited-timeline gap.

    Abandonment covers two categorically different situations, and an
    application that treats them alike gets one of them wrong. ``EVENT_LIMIT``
    and ``FETCH_FAILED`` say only that nio stopped: the missing history was
    never shown to be gone, and a client that is willing to spend more than
    nio's bounded walk may still be able to fetch it. ``UNVERIFIABLE`` says the
    walk cannot be made to work at all, so those events will never arrive.

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

    UNVERIFIABLE = "unverifiable"
    """Continuity cannot be proven: pagination stalled or ran out before
    reaching the target, or there was no baseline to walk from. Retrying the
    same walk cannot help, so the missing events are permanently lost."""
