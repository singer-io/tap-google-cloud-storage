from google.api_core.exceptions import (
    InternalServerError,
    BadGateway,
    ServiceUnavailable,
    GatewayTimeout,
    TooManyRequests,
)


class GCSError(Exception):
    """Class representing a generic GCS error."""

    def __init__(self, message=None):
        super().__init__(message)
        self.message = message


class GCSBackoffError(GCSError):
    """Class representing errors that should trigger a backoff retry."""
    pass


class GCSInternalServerError(GCSBackoffError):
    """Class representing 500 Internal Server Error."""
    pass


class GCSBadGatewayError(GCSBackoffError):
    """Class representing 502 Bad Gateway."""
    pass


class GCSServiceUnavailableError(GCSBackoffError):
    """Class representing 503 Service Unavailable."""
    pass


class GCSGatewayTimeoutError(GCSBackoffError):
    """Class representing 504 Gateway Timeout."""
    pass


class GCSRateLimitError(GCSError):
    """Class representing 429 Too Many Requests."""
    pass


class GCSConnectionError(GCSBackoffError):
    """Class representing transient connection errors."""
    pass


class GCSConnectionResetError(GCSBackoffError):
    """Class representing connection reset errors."""
    pass


# Exceptions that are safe to retry (transient server-side / connection errors)
RETRYABLE_EXCEPTIONS = (
    InternalServerError,   # 500
    BadGateway,            # 502
    ServiceUnavailable,    # 503
    GatewayTimeout,        # 504
    ConnectionError,
    ConnectionResetError,
    GCSBackoffError,
)

# Rate-limit exceptions retried with a dedicated backoff policy
RATE_LIMIT_EXCEPTIONS = (
    TooManyRequests,       # 429
    GCSRateLimitError,
)
