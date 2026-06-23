"""Exception mapping at the handler boundary — the supported way to control retry
classification (RetryMiddleware does not accept custom predicates; see docs §7).

Translate downstream/library errors into our transient/permanent base classes so
the type-based classifier routes them correctly.
"""

from __future__ import annotations

from .errors import DownstreamUnavailable, PermanentError

# 429 (rate limited) + 5xx that are typically transient. Everything else 4xx is a
# client error we caused → permanent.
RETRYABLE_HTTP_STATUS = frozenset({429, 502, 503, 504})


def map_http_status(status: int, detail: str = "") -> Exception:
    """Map an HTTP status code to a rabbitkit-classifiable exception by TYPE."""
    if status in RETRYABLE_HTTP_STATUS:
        return DownstreamUnavailable(f"HTTP {status}: {detail}".rstrip(": "))
    return PermanentError(f"HTTP {status}: {detail}".rstrip(": "))
