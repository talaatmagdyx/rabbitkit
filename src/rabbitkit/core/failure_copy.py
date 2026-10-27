"""Retry and dead-letter copies of a failed delivery, with failure metadata.

Transport-free (hard invariant 1). Shared by ``RetryMiddleware`` (retry
copies and its terminal DLQ copy) and :class:`~rabbitkit.core.dead_letter.DeadLetterCopier`
(the DLQ copy for failures that never reach ``RetryMiddleware``), so every
copy a consumer of the DLQ sees carries the same headers.

A copy keeps the WIRE body and ``content_encoding`` (G-07: an ``on_receive``
hook such as decompression may have rewritten ``message.body`` in place),
every AMQP property except ``expiration`` (moved to
``x-rabbitkit-original-expiration``), every header, and a stable
``message_id``. It is published ``mandatory=True`` straight to the target
queue through the default exchange.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.sanitizer import ErrorSanitizer
from rabbitkit.core.types import ClassifiedError, FailureCategory, MessageEnvelope

#: Error-message header cap: an exception with a huge str() cannot bloat
#: every copy.
ERROR_MESSAGE_MAX_LEN = 512

#: ``RetryConfig.retry_header``'s default.
DEFAULT_RETRY_HEADER = "x-rabbitkit-retry-count"

# Retry/DLQ metadata headers (spec §13.4 / §14.1).
H_ORIGINAL_QUEUE = "x-rabbitkit-original-queue"
H_ORIGINAL_MESSAGE_ID = "x-rabbitkit-original-message-id"
H_ORIGINAL_EXPIRATION = "x-rabbitkit-original-expiration"
H_FIRST_FAILED_AT = "x-rabbitkit-first-failed-at"
H_ATTEMPTS = "x-rabbitkit-attempts"
H_ERROR_CLASS = "x-rabbitkit-error-class"
H_FAILURE_CATEGORY = "x-rabbitkit-failure-category"
H_DEAD_LETTERED_AT = "x-rabbitkit-dead-lettered-at"
H_ERROR_CATEGORY = "x-rabbitkit-error-category"


def header_text(value: Any) -> str:
    """A header as text (bytes decoded); anything else reads as ``""``."""
    if isinstance(value, bytes):
        value = value.decode(errors="replace")
    return value if isinstance(value, str) else ""


def header_int(value: Any) -> int:
    """A header as a non-negative int; malformed values read as 0."""
    if isinstance(value, bool):
        return 0
    try:
        return max(0, int(value))
    except (TypeError, ValueError):
        return 0


def parse_iso(value: Any) -> datetime | None:
    """An ISO-8601 header as an aware UTC datetime, or ``None``.

    A future timestamp is clamped to now (clock skew or a hostile producer
    must not create negative ages)."""
    text = header_text(value)
    if isinstance(value, datetime):
        parsed = value
    elif not text:
        return None
    else:
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    now = datetime.now(UTC)
    return min(parsed, now)


def stable_message_id(message: RabbitMessage, headers: dict[str, Any]) -> str:
    """The message id every copy (retry and DLQ) carries.

    Recorded once, on the first handoff, as
    ``x-rabbitkit-original-message-id``. A message published without a
    ``message_id`` gets a fresh one at that point — otherwise the client
    library stamps a random id on EVERY republish and the attempts of one
    logical message can't be correlated (12.2).
    """
    original = header_text(headers.get(H_ORIGINAL_MESSAGE_ID))
    if not original:
        original = message.message_id or uuid.uuid4().hex
        headers[H_ORIGINAL_MESSAGE_ID] = original
    return message.message_id or original


def stamp_failure(
    headers: dict[str, Any],
    message: RabbitMessage,
    exc: BaseException,
    classified: ClassifiedError,
    category: FailureCategory,
    attempts: int,
    now: str,
    sanitizer: ErrorSanitizer,
) -> None:
    """Write the CURRENT attempt's failure metadata onto *headers*."""
    if "x-rabbitkit-original-exchange" not in headers:
        headers["x-rabbitkit-original-exchange"] = message.exchange
    if "x-rabbitkit-original-routing-key" not in headers:
        headers["x-rabbitkit-original-routing-key"] = message.routing_key
    if H_ORIGINAL_QUEUE not in headers:
        headers[H_ORIGINAL_QUEUE] = ""  # set by broker at consume time
    # Item 7 / plan §5.3: allowlisted category/code + bounded, REDACTED
    # summary. ``error_detail="omit"`` writes no message text at all;
    # ``"raw"`` is the legacy opt-in. A previous attempt's message header
    # is dropped so a sanitized copy cannot carry forward stale raw text.
    headers.pop("x-rabbitkit-error-message", None)
    sanitized = sanitizer.sanitize(exc, classified.severity)
    headers.update(sanitized.as_headers())
    # The failure taxonomy (retryable / permanent / invalid_message /
    # unknown_outcome / infrastructure / shutdown), not just the severity.
    headers[H_ERROR_CATEGORY] = classified.category.value
    headers[H_ERROR_CLASS] = sanitized.code
    headers[H_FAILURE_CATEGORY] = category.value
    # Attempts = handler executions so far, including this one. Never
    # lowered: a copy that already claims more keeps its claim.
    headers[H_ATTEMPTS] = max(attempts, header_int(headers.get(H_ATTEMPTS, 0)))
    # first-failed-at is set once and preserved across every retry;
    # last-failed-at is overwritten on every failure.
    if parse_iso(headers.get(H_FIRST_FAILED_AT)) is None:
        headers[H_FIRST_FAILED_AT] = now
    headers["x-rabbitkit-last-failed-at"] = now
    if message.expiration and H_ORIGINAL_EXPIRATION not in headers:
        headers[H_ORIGINAL_EXPIRATION] = message.expiration


def copy_envelope(
    message: RabbitMessage, *, routing_key: str, headers: dict[str, Any], message_id: str
) -> MessageEnvelope:
    """A republish of *message* that keeps every AMQP property except
    ``expiration`` (see the note in the body).

    The body and ``content_encoding`` are the WIRE values (G-07): an
    ``on_receive`` hook may have rewritten ``message.body`` in place
    (``CompressionMiddleware`` decompresses it but leaves
    ``content_encoding="gzip"``). Republishing that decoded body under
    the original encoding made every retried compressed message
    undecodable on its next delivery, so it was dead-lettered instead of
    retried. The wire bytes are also what a signature covers.
    """
    return MessageEnvelope(
        routing_key=routing_key,
        body=message.wire_body,
        exchange="",  # direct to the queue by name
        headers=headers,
        message_id=message_id,
        correlation_id=message.correlation_id,
        content_type=message.content_type or "application/octet-stream",
        content_encoding=message.wire_content_encoding,
        # Preserve the remaining original message properties -- these used
        # to be silently dropped on every retry republish, so e.g. a
        # priority-queue message lost its priority on its first retry, and
        # an RPC request's reply_to/type/app_id/user_id never survived
        # long enough for the eventual (retried) reply to route back.
        reply_to=message.reply_to,
        priority=message.priority,
        # The producer's timestamp is the event time; a consumer that
        # orders or ages messages by it must see the original (R-D5).
        timestamp=message.timestamp,
        # A known transient (1) stays transient; unknown stays persistent.
        delivery_mode=message.delivery_mode if message.delivery_mode in (1, 2) else 2,
        # expiration is deliberately NOT preserved: a producer-set
        # per-message TTL shorter than the delay tier's x-message-ttl
        # would expire the message INSIDE the delay queue and dead-letter
        # it back to the source early, collapsing the whole backoff
        # ladder into a fast retry storm (and on the DLQ it would silently
        # delete the failure record). The original value is kept in the
        # x-rabbitkit-original-expiration header.
        type=message.type,
        app_id=message.app_id,
        user_id=message.user_id,
        # M4: mandatory so a runtime-deleted/missing queue comes back as
        # RETURNED (outcome not-ok) instead of being broker-confirmed into
        # the void. Requires publisher confirms + basic.return handling,
        # which both transports wire up.
        mandatory=True,
    )


def dead_letter_envelope(
    message: RabbitMessage,
    *,
    dlq: str,
    exc: BaseException,
    classified: ClassifiedError,
    category: FailureCategory,
    attempts: int,
    sanitizer: ErrorSanitizer,
    header_defaults: dict[str, Any] | None = None,
) -> MessageEnvelope:
    """The metadata-stamped DLQ copy of *message*, addressed to queue *dlq*.

    *header_defaults* fill headers the delivery lacks (or carries empty) on
    the copy only; the delivery itself is not changed."""
    headers = dict(message.headers)
    for name, value in (header_defaults or {}).items():
        if headers.get(name) in (None, ""):
            headers[name] = value
    message_id = stable_message_id(message, headers)
    now = datetime.now(UTC).isoformat()
    stamp_failure(headers, message, exc, classified, category, attempts, now, sanitizer)
    headers[H_DEAD_LETTERED_AT] = now
    return copy_envelope(message, routing_key=dlq, headers=headers, message_id=message_id)


__all__ = [
    "DEFAULT_RETRY_HEADER",
    "ERROR_MESSAGE_MAX_LEN",
    "copy_envelope",
    "dead_letter_envelope",
    "header_int",
    "header_text",
    "parse_iso",
    "stable_message_id",
    "stamp_failure",
]
