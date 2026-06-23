"""Exception hierarchy carrying retry severity BY TYPE.

rabbitkit's classifier is type-based (see docs §7). It checks isinstance against:
    TRANSIENT_ERRORS = (ConnectionResetError, BrokenPipeError,
                        ConnectionAbortedError, TimeoutError, EOFError, OSError)
    PERMANENT_ERRORS = (json.JSONDecodeError, KeyError, ValueError,
                        TypeError, UnicodeDecodeError, AttributeError)

So a custom transient error MUST subclass a recognized transient base (OSError),
and a permanent error MUST subclass a permanent base (ValueError). Anything that
matches neither falls through to RetryConfig.unknown_policy (PERMANENT) and is
NOT retried.
"""

from __future__ import annotations


class TransientError(OSError):
    """Retry me. OSError ∈ TRANSIENT_ERRORS → classified TRANSIENT."""


class PermanentError(ValueError):
    """Do not retry. ValueError ∈ PERMANENT_ERRORS → classified PERMANENT."""


class DownstreamUnavailable(TransientError):
    """A dependency is temporarily down (timeout, 5xx, connection reset)."""


class InvalidTenant(PermanentError):
    """Unknown/unauthorized tenant — no amount of retrying fixes this."""


class DuplicateOrder(PermanentError):
    """Business rule: a re-submitted order is terminal, not retryable."""
