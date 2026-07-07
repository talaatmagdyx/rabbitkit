"""Error classification — transport-agnostic.

Transport-specific exception tuples (pika.exceptions.StreamLostError, etc.)
live in sync/connection.py and async_/connection.py, NOT here.

This module provides:
- Generic stdlib exception tuples for classification
- Pluggable predicate-based classification
- Configurable unknown_policy (default=PERMANENT)

See Contract 7 in the plan for evaluation order and rationale.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Sequence

# Re-export: defined in message.py to avoid an import cycle (errors -> types -> message).
# The `as` form marks it an explicit re-export for mypy --strict.
from rabbitkit.core.message import SettlementError as SettlementError
from rabbitkit.core.types import ClassifiedError, ErrorSeverity

# ── Missing DI dependency (H10) ──────────────────────────────────────────
# Defined before PERMANENT_ERRORS below so it can be listed there directly.


class MissingDependencyError(Exception):
    """Raised at message-processing time when a required ``Header()`` /
    ``Path()`` / ``Context()`` marker's value is absent from the incoming
    message and no default is available — neither on the marker itself
    (``Header("x-tenant", default=...)``) nor the handler's own parameter
    default (``tenant: Annotated[str | None, Header("x-tenant")] = None``).

    Names the specific parameter and marker so the failure is immediately
    actionable — unlike the bare ``KeyError`` this replaces, which looked
    identical to a handler bug (e.g. indexing a dict) and gave no indication
    which DI marker was the culprit. Classified PERMANENT by
    :func:`classify_error` (see ``PERMANENT_ERRORS`` below), matching the
    ``KeyError`` classification it replaces: a missing required value means
    the message itself is malformed for this handler — retrying will not fix
    it, so it settles straight to the DLQ rather than looping.
    """

    def __init__(self, marker_repr: str, param_name: str) -> None:
        super().__init__(
            f"{marker_repr} for parameter {param_name!r} is required but missing from the "
            "message, and no default is available (neither on the marker itself nor as a "
            "Python parameter default)."
        )
        self.param_name = param_name


# Generic (stdlib) error categories — transport layers extend these
TRANSIENT_ERRORS: tuple[type[BaseException], ...] = (
    TimeoutError,
    EOFError,  # NOT an OSError subclass — must be listed explicitly
    OSError,  # covers ConnectionResetError, BrokenPipeError, ConnectionAbortedError
)

PERMANENT_ERRORS: tuple[type[BaseException], ...] = (
    json.JSONDecodeError,
    KeyError,
    ValueError,
    TypeError,
    UnicodeDecodeError,
    AttributeError,
    MissingDependencyError,
)

# Type alias for error predicates
ErrorPredicate = Callable[[BaseException], bool | None]


def classify_error(
    exc: BaseException,
    *,
    predicates: Sequence[ErrorPredicate] = (),
    transient: tuple[type[BaseException], ...] = TRANSIENT_ERRORS,
    permanent: tuple[type[BaseException], ...] = PERMANENT_ERRORS,
    unknown_policy: ErrorSeverity = ErrorSeverity.PERMANENT,
) -> ClassifiedError:
    """Classify exception severity.

    Evaluation order:
    1. User predicates (first non-None wins)
    2. transient tuple (isinstance check)
    3. permanent tuple (isinstance check)
    4. unknown_policy (configurable, default=PERMANENT)

    DEFAULT IS PERMANENT, NOT TRANSIENT.
    Reason: unknown errors include malformed payloads, business validation
    failures, handler bugs, and schema mismatches. Treating these as
    transient creates retry storms. Network/connection errors are already
    in the transient tuple.

    Override per-route: RetryMiddleware accepts unknown_policy to change
    per route.

    Args:
        exc: The exception to classify.
        predicates: User-provided classification functions.
            Return True=transient, False=permanent, None=no opinion.
        transient: Exception types considered transient.
        permanent: Exception types considered permanent.
        unknown_policy: Severity for unclassified exceptions.

    Returns:
        ClassifiedError with severity, original exception, and reason.
    """
    # 1. User predicates (first non-None wins)
    for predicate in predicates:
        result = predicate(exc)
        if result is True:
            return ClassifiedError(
                severity=ErrorSeverity.TRANSIENT,
                original=exc,
                reason=f"predicate classified as transient: {type(exc).__name__}",
            )
        if result is False:
            return ClassifiedError(
                severity=ErrorSeverity.PERMANENT,
                original=exc,
                reason=f"predicate classified as permanent: {type(exc).__name__}",
            )

    # 2. Transient tuple (isinstance check)
    if isinstance(exc, transient):
        return ClassifiedError(
            severity=ErrorSeverity.TRANSIENT,
            original=exc,
            reason=f"transient error: {type(exc).__name__}",
        )

    # 3. Permanent tuple (isinstance check)
    if isinstance(exc, permanent):
        return ClassifiedError(
            severity=ErrorSeverity.PERMANENT,
            original=exc,
            reason=f"permanent error: {type(exc).__name__}",
        )

    # 4. Unknown policy
    return ClassifiedError(
        severity=unknown_policy,
        original=exc,
        reason=f"unknown error classified as {unknown_policy.value}: {type(exc).__name__}",
    )


# ── Configuration error (single canonical location) ─────────────────────


class ConfigurationError(Exception):
    """Raised for invalid configuration detected at registration time.

    Single canonical class for all registration-time misconfigurations (route
    conflicts, invalid handler signatures, bad retry/ack combinations). Both
    ``core/route.py`` and ``di/resolver.py`` raise this; tests/users can catch
    one type regardless of import source.
    """


# ── Unsafe topology error ────────────────────────────────────────────────


class UnsafeTopologyError(ConfigurationError):
    """Raised at startup when ``RejectWithoutDLXPolicy.ERROR`` is active and a
    route can ``reject(requeue=False)`` but its queue has no dead-letter
    exchange — RabbitMQ would silently discard rejected messages.

    Subclasses :class:`ConfigurationError` so existing catch-alls for
    registration/startup misconfiguration keep working.
    """


# ── Construction-time validation errors ──────────────────────────────────
# Both dual-inherit ValueError: every one of these sites historically raised
# a bare ValueError, so ``except ValueError`` (and every existing
# ``pytest.raises(ValueError)``) keeps working — the custom types only ADD a
# way to catch rabbitkit-specific validation precisely.


class ConfigValidationError(ConfigurationError, ValueError):
    """An invalid value was passed to a rabbitkit config object or
    constructor — ``RabbitConfig`` sub-configs (``RetryConfig(max_retries=-1)``,
    ``WorkerConfig(max_queue_size=-1)``, …) and AMQP short-string fields
    (queue/exchange/routing-key names that are too long or contain control
    characters, including on ``MessageEnvelope``).

    Raised at construction time (``__post_init__``), so a bad value fails
    where it's written, not at first use.
    """


class TopologyValidationError(ConfigurationError, ValueError):
    """An invalid ``RabbitQueue`` / ``RabbitExchange`` declaration — a
    combination RabbitMQ itself would reject or silently misbehave on
    (non-durable quorum queue, priorities on a stream, ``delivery_limit`` on
    a classic queue, non-positive ``consumer_timeout``, …).

    Raised at model construction time, before anything touches the broker.
    """


class MessageTooLargeError(ValueError):
    """A publish was rejected client-side because the body exceeds
    ``PublisherConfig.max_message_bytes`` (default 16 MiB, mirroring the
    server's ``max_message_size`` default).

    Raised *before* the bytes hit the wire: the server would reject the
    message anyway, but with a channel exception that kills the (pooled)
    publisher channel and corrupts sibling in-flight publishes. Subclasses
    ``ValueError`` for backward compatibility with earlier releases that
    raised it directly.
    """


# ── Runtime API-misuse errors ────────────────────────────────────────────
# Both dual-inherit RuntimeError for the same backward-compat reason.


class BrokerNotStartedError(RuntimeError):
    """A broker method that needs a live transport (``publish()``, RPC,
    topology helpers) was called before ``start()`` (or after ``stop()``).
    """



# ── Publish error (opt-in) ───────────────────────────────────────────────


class PublishError(Exception):
    """Raised by ``PublishOutcome.raise_for_status()`` on a failed publish.

    ``broker.publish()`` never raises on its own — it returns a
    ``PublishOutcome`` so callers can decide how to handle NACKED / TIMEOUT /
    RETURNED / ERROR. Code that prefers exceptions can opt in with
    ``broker.publish(...).raise_for_status()``. Carries the ``outcome`` for
    inspection (status, routing_key, underlying error).
    """

    def __init__(self, outcome: object) -> None:
        self.outcome = outcome
        super().__init__(str(outcome))


# ── Backpressure error ───────────────────────────────────────────────────


class BackpressureError(Exception):
    """Raised when publish-side flow control blocks a publish attempt.

    Only raised when ``BackpressureConfig.on_blocked == "raise"``.
    """
