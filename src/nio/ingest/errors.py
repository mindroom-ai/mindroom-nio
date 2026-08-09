class BatchIntegrityError(ValueError):
    """Raised when a durable batch identity does not match its contents."""


class FreshIngestionRequired(RuntimeError):
    """Raised when an unmarked nonempty store requires explicit initialization."""


class JournalConflictError(RuntimeError):
    """Raised when a journal compare-and-swap or ordered operation is stale."""


class JournalIntegrityError(ValueError):
    """Raised when authenticated or relational journal state is inconsistent."""
