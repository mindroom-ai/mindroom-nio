from collections.abc import Iterable
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


def normalize_abandonment_reasons(value: object) -> frozenset[RecoveryAbandonment]:
    """Return canonical abandonment causes for singular or legacy input.

    A singular enum has to be handled before generic iterables because the
    enum is also a string. Invalid values intentionally become ``UNKNOWN``:
    retaining that uncertainty is safer than silently dropping a loss.
    """
    if isinstance(value, RecoveryAbandonment):
        return frozenset({value})
    if isinstance(value, str):
        try:
            return frozenset({RecoveryAbandonment(value)})
        except ValueError:
            return frozenset({RecoveryAbandonment.UNKNOWN})
    if not isinstance(value, Iterable):
        return frozenset({RecoveryAbandonment.UNKNOWN})

    reasons: set[RecoveryAbandonment] = set()
    for item in value:
        if isinstance(item, RecoveryAbandonment):
            reasons.add(item)
            continue
        if isinstance(item, str):
            try:
                reasons.add(RecoveryAbandonment(item))
                continue
            except ValueError:
                pass
        reasons.add(RecoveryAbandonment.UNKNOWN)
    return frozenset(reasons)
