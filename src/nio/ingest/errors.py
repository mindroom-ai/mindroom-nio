class BatchIntegrityError(ValueError):
    """Raised when a durable batch identity does not match its contents."""


class FreshIngestionRequired(RuntimeError):
    """Raised when an unmarked nonempty store requires explicit initialization."""


class _MarkedStoreRequiresSqlite(FreshIngestionRequired):
    """Signal that a configured marked-store probe must retry as SqliteStore."""


class JournalConflictError(RuntimeError):
    """Raised when a journal compare-and-swap or ordered operation is stale."""


class JournalCapacityError(RuntimeError):
    """Raised when freshly prepared output exceeds a hard resource bound."""


class JournalIntegrityError(ValueError):
    """Raised when authenticated or relational journal state is inconsistent."""
