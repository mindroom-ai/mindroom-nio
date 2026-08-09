class BatchIntegrityError(ValueError):
    """Raised when a durable batch identity does not match its contents."""
