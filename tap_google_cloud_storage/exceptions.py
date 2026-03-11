from google.api_core.exceptions import (
    InternalServerError,
    BadGateway,
    ServiceUnavailable,
    GatewayTimeout,
    TooManyRequests,
    GoogleAPICallError,
)


class GCSError(Exception):
    """Class representing a generic GCS error."""

    def __init__(self, message=None):
        super().__init__(message)
        self.message = message


class GCSBackoffError(GCSError):
    """Class representing all server errors that should trigger a backoff retry."""
    pass


class GCSInternalServerError(GCSBackoffError):
    """Class representing 500 status code."""
    pass


class GCSBadGatewayError(GCSBackoffError):
    """Class representing 502 status code."""
    pass


class GCSServiceUnavailableError(GCSBackoffError):
    """Class representing 503 status code."""
    pass


class GCSGatewayTimeoutError(GCSBackoffError):
    """Class representing 504 status code."""
    pass


class GCSRateLimitError(GCSError):
    """Class representing 429 status code."""
    pass


class GCSConnectionError(GCSBackoffError):
    """Class representing transient connection errors."""
    pass


class GCSConnectionResetError(GCSBackoffError):
    """Class representing connection reset errors."""
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    InternalServerError: {
        "raise_exception": GCSInternalServerError,
        "message": "The server encountered an unexpected condition which prevented"
            " it from fulfilling the request."
    },
    BadGateway: {
        "raise_exception": GCSBadGatewayError,
        "message": "Server received an invalid response."
    },
    ServiceUnavailable: {
        "raise_exception": GCSServiceUnavailableError,
        "message": "API service is currently unavailable."
    },
    GatewayTimeout: {
        "raise_exception": GCSGatewayTimeoutError,
        "message": "The server did not receive a timely response from an upstream server."
    },
    TooManyRequests: {
        "raise_exception": GCSRateLimitError,
        "message": "The API rate limit has been exceeded."
    },
    ConnectionError: {
        "raise_exception": GCSConnectionError,
        "message": "A connection error occurred."
    },
    ConnectionResetError: {
        "raise_exception": GCSConnectionResetError,
        "message": "The connection was reset."
    },
}

# Tuple of raw exceptions to catch before translating
RAW_EXCEPTIONS = tuple(ERROR_CODE_EXCEPTION_MAPPING.keys())


def raise_for_error(ex):
    """Translate a raw Google API / connection exception into a custom GCS exception.

    Checks the exception type against ERROR_CODE_EXCEPTION_MAPPING, and raises
    the corresponding GCSBackoffError or GCSRateLimitError subclass.
    For unmapped 5xx Google API errors, falls back to GCSBackoffError.
    Otherwise re-raises the original exception.
    """
    for raw_exc_type, mapping in ERROR_CODE_EXCEPTION_MAPPING.items():
        if isinstance(ex, raw_exc_type):
            exc_class = mapping["raise_exception"]
            message = f"Error: {ex}, {mapping['message']}"
            raise exc_class(message) from ex

    # Fallback: unmapped Google API 5xx errors → GCSBackoffError
    if isinstance(ex, GoogleAPICallError) and hasattr(ex, 'code') and 500 <= ex.code < 600:
        raise GCSBackoffError(str(ex)) from ex

    raise ex
