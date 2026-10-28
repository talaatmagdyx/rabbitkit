"""Headers only rabbitkit (or the broker) may set (G-19).

``RetryMiddleware`` decides how many retries a message has left from
headers it carries: ``x-rabbitkit-retry-count`` (unless
``RetryConfig.trust_retry_header=False``), the broker's ``x-death``
history for ``x-rabbitkit-original-queue``, and
``x-rabbitkit-first-failed-at`` (``max_retry_age``). An application publish
that carries them — forged, or copied from a consumed message — can make a
new message skip its retries. ``PublisherConfig.strip_reserved_headers``
removes them from application publishes (``broker.publish()`` and the bulk
APIs built on it). rabbitkit's own retry, DLQ and replay republishes do not
go through ``broker.publish()`` and keep them.
"""

from __future__ import annotations

import dataclasses
import logging
from collections.abc import Iterable, Mapping
from typing import Any

from rabbitkit.core.types import MessageEnvelope

__all__ = ["RESERVED_HEADERS", "ReservedHeaderStripper", "strip_reserved_headers"]

logger = logging.getLogger(__name__)

#: Retry / dead-letter / replay bookkeeping written by rabbitkit, plus the
#: broker's ``x-death`` history. Signing headers are not listed: a signing
#: middleware adds them after the strip.
RESERVED_HEADERS = frozenset(
    {
        "x-death",
        "x-rabbitkit-retry-count",
        "x-rabbitkit-attempts",
        "x-rabbitkit-first-failed-at",
        "x-rabbitkit-last-failed-at",
        "x-rabbitkit-original-queue",
        "x-rabbitkit-original-exchange",
        "x-rabbitkit-original-routing-key",
        "x-rabbitkit-original-message-id",
        "x-rabbitkit-original-expiration",
        "x-rabbitkit-error",
        "x-rabbitkit-error-category",
        "x-rabbitkit-error-class",
        "x-rabbitkit-error-message",
        "x-rabbitkit-error-type",
        "x-rabbitkit-failure-category",
        "x-rabbitkit-dead-lettered-at",
        "x-rabbitkit-replay-count",
        "x-rabbitkit-replayed-at",
        "x-rabbitkit-replayed-by",
        "x-rabbitkit-previous-retry-count",
    }
)


def strip_reserved_headers(headers: Mapping[str, Any], extra: Iterable[str] = ()) -> dict[str, Any] | None:
    """*headers* without the reserved ones (and any in *extra*), or ``None``
    when nothing had to be removed (the caller keeps its object, no copy)."""
    extra_set = frozenset(extra)
    if not any(name in RESERVED_HEADERS or name in extra_set for name in headers):
        return None
    return {k: v for k, v in headers.items() if k not in RESERVED_HEADERS and k not in extra_set}


class ReservedHeaderStripper:
    """Applies :func:`strip_reserved_headers` to outgoing envelopes.

    Used by ``broker.publish()`` when ``PublisherConfig.strip_reserved_headers``
    is on. Logs one WARNING per stripped header name for the life of the
    broker (bounded by the reserved set), so a producer forging or forwarding
    them is visible without flooding the log.
    """

    __slots__ = ("_extra", "_warned")

    def __init__(self, extra: Iterable[str] = ()) -> None:
        self._extra = frozenset(extra) - RESERVED_HEADERS
        self._warned: set[str] = set()

    def __call__(self, envelope: MessageEnvelope) -> MessageEnvelope:
        headers = strip_reserved_headers(envelope.headers, self._extra)
        if headers is None:
            return envelope
        removed = sorted(set(envelope.headers) - set(headers) - self._warned)
        if removed:
            self._warned.update(removed)
            logger.warning(
                "Stripped rabbitkit-reserved header(s) %s from an application publish "
                "(PublisherConfig.strip_reserved_headers=True); only rabbitkit and the broker set them. "
                "Logged once per header name.",
                ", ".join(removed),
            )
        return dataclasses.replace(envelope, headers=headers)
