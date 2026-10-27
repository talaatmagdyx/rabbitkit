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

import asyncio
import json
from collections.abc import Callable, Sequence

# Re-export: defined in message.py to avoid an import cycle (errors -> types -> message).
# The `as` form marks it an explicit re-export for mypy --strict.
from rabbitkit.core.message import SettlementError as SettlementError
from rabbitkit.core.types import ClassifiedError, ErrorCategory, ErrorSeverity

#: Attribute an exception (class or instance) carries to declare its own
#: :class:`ErrorCategory`. Checked before predicates and the type tuples.
ERROR_CATEGORY_ATTR = "rabbitkit_error_category"

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
    it, so it settles straight to the DLQ rather than looping. Its category
    is ``INVALID_MESSAGE``.
    """

    rabbitkit_error_category = ErrorCategory.INVALID_MESSAGE

    def __init__(self, marker_repr: str, param_name: str) -> None:
        super().__init__(
            f"{marker_repr} for parameter {param_name!r} is required but missing from the "
            "message, and no default is available (neither on the marker itself nor as a "
            "Python parameter default)."
        )
        self.param_name = param_name


# ── Handler deadline (CONSUMER-006) ─────────────────────────────────────


class HandlerTimeoutError(TimeoutError):
    """A handler exceeded its processing deadline and was abandoned.

    Raised ONLY by a deadline rabbitkit enforces itself
    (``ConsumerConfig.processing_timeout`` or
    :class:`~rabbitkit.middleware.timeout.TimeoutMiddleware`). A
    ``TimeoutError`` raised by the handler's own code (an HTTP client
    timeout, say) is NOT relabelled as this: it stays an ordinary transient
    error.

    The outcome is unknown: the handler was cancelled (async) or abandoned
    (sync) part-way, so some, all, or none of its side effects may have
    happened. ``outcome_unknown`` is True on every instance and
    :func:`classify_error` copies it to :attr:`ClassifiedError.outcome_unknown`
    and the reason string. It is classified ``TRANSIENT`` (retrying is
    reasonable) -- which is only safe for idempotent handlers.
    """

    outcome_unknown: bool = True

    def __init__(self, timeout_seconds: float) -> None:
        super().__init__(
            f"Handler exceeded timeout of {timeout_seconds}s; outcome unknown (side effects may have happened)"
        )
        self.timeout_seconds = timeout_seconds


# ── Typed exceptions for the failure taxonomy ───────────────────────────


class InvalidMessageError(ValueError):
    """Raise from a handler when the message itself is unusable: it cannot
    be decoded or validated, or a required field is missing.

    Category ``INVALID_MESSAGE``, severity ``PERMANENT``: never retried,
    dead-lettered with ``x-rabbitkit-error-category: invalid_message``.
    Subclasses ``ValueError`` so existing ``except ValueError`` code keeps
    working. rabbitkit applies the same category to body-decode failures and
    ``on_receive`` failures (decompression, signature) on its own.
    """

    rabbitkit_error_category = ErrorCategory.INVALID_MESSAGE


class EventMetadataError(InvalidMessageError):
    """Event metadata is missing or invalid (:mod:`rabbitkit.core.event_metadata`).

    Raised when building :class:`~rabbitkit.core.event_metadata.EventMetadata`
    (a producer bug, raised before anything is published) and when reading
    it from a delivery (``INVALID_MESSAGE``: dead-lettered, never retried).
    ``fields`` names the offending fields; the message never contains their
    values, which may carry payload data.
    """

    def __init__(self, message: str, *, fields: tuple[str, ...] = ()) -> None:
        super().__init__(message)
        self.fields = fields


class InboundMessageTooLargeError(InvalidMessageError):
    """A delivery's raw body exceeds ``ConsumerConfig.max_body_bytes``.

    ``INVALID_MESSAGE``: the delivery is dead-lettered without running any
    middleware or the handler.
    """

    def __init__(self, size: int, limit: int) -> None:
        super().__init__(f"Delivered body ({size} bytes) exceeds ConsumerConfig.max_body_bytes ({limit})")
        self.size = size
        self.limit = limit


class InfrastructureError(Exception):
    """Raise (or wrap) from a handler when a dependency is unavailable:
    a database, an HTTP service, a cache.

    Category ``INFRASTRUCTURE``, severity ``TRANSIENT``: retried with the
    route's ``RetryConfig``, and labelled separately from ordinary transient
    failures in headers and metrics, so an outage of one dependency is
    visible as such.
    """

    rabbitkit_error_category = ErrorCategory.INFRASTRUCTURE


def mark_invalid_message(exc: BaseException) -> None:
    """Tag *exc* as ``INVALID_MESSAGE`` unless it already declares a category.

    Used by the pipeline on body-decode and ``on_receive`` failures. An
    exception type that refuses new attributes is left alone (it then
    classifies as before, which is still terminal for decode errors).
    """
    if isinstance(getattr(exc, ERROR_CATEGORY_ATTR, None), ErrorCategory):
        return
    try:
        setattr(exc, ERROR_CATEGORY_ATTR, ErrorCategory.INVALID_MESSAGE)
    except (AttributeError, TypeError):
        pass


# ── Infrastructure errors registered by the transports ──────────────────
# core/ never imports a transport. sync/ and async_/ register their client
# library's connection and channel errors here (the brokers do it at
# construction), so classify_error() labels them INFRASTRUCTURE -- and
# retries them -- without core knowing pika or aio-pika exist.

_infrastructure_errors: tuple[type[BaseException], ...] = ()


def register_infrastructure_errors(*types: type[BaseException]) -> None:
    """Add exception types :func:`classify_error` treats as
    ``INFRASTRUCTURE`` (severity ``TRANSIENT``). Idempotent; order kept."""
    global _infrastructure_errors
    new = tuple(t for t in types if t not in _infrastructure_errors)
    if new:
        _infrastructure_errors = _infrastructure_errors + new


def infrastructure_errors() -> tuple[type[BaseException], ...]:
    """The exception types currently registered as ``INFRASTRUCTURE``."""
    return _infrastructure_errors


#: Never a handler failure: requeue and let the cancellation propagate.
SHUTDOWN_ERRORS: tuple[type[BaseException], ...] = (asyncio.CancelledError, KeyboardInterrupt, SystemExit)

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

# Type alias for error predicates: True = transient, False = permanent,
# an ErrorCategory = that category (and its severity), None = no opinion.
ErrorPredicate = Callable[[BaseException], bool | ErrorCategory | None]


def classify_error(
    exc: BaseException,
    *,
    predicates: Sequence[ErrorPredicate] = (),
    transient: tuple[type[BaseException], ...] = TRANSIENT_ERRORS,
    permanent: tuple[type[BaseException], ...] = PERMANENT_ERRORS,
    unknown_policy: ErrorSeverity = ErrorSeverity.PERMANENT,
    infrastructure: tuple[type[BaseException], ...] | None = None,
) -> ClassifiedError:
    """Classify an exception: severity (retry or not) and category (why).

    Evaluation order:
    0. Shutdown (``CancelledError``, ``KeyboardInterrupt``, ``SystemExit``)
       → ``SHUTDOWN``
    1. A category the exception declares itself (``rabbitkit_error_category``
       on its class or instance: :class:`InvalidMessageError`,
       :class:`InfrastructureError`, :class:`MissingDependencyError`, and
       body-decode / ``on_receive`` failures, which the pipeline tags
       ``INVALID_MESSAGE``)
    2. User predicates (first non-None wins; ``True`` / ``False`` /
       an :class:`ErrorCategory`)
    3. infrastructure tuple (default: what the transports registered via
       :func:`register_infrastructure_errors`) → ``INFRASTRUCTURE``
    4. transient tuple (isinstance check) → ``RETRYABLE``
    5. permanent tuple (isinstance check) → ``PERMANENT``
    6. unknown_policy (configurable, default=PERMANENT)

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
            Return True=transient, False=permanent, an ErrorCategory, or
            None=no opinion.
        transient: Exception types considered transient.
        permanent: Exception types considered permanent.
        unknown_policy: Severity for unclassified exceptions.
        infrastructure: Exception types labelled ``INFRASTRUCTURE``
            (transient). ``None`` uses the registered set.

    Returns:
        ClassifiedError with severity, category, original exception, and
        reason. ``outcome_unknown`` is set when *exc* carries a truthy
        ``outcome_unknown`` attribute (:class:`HandlerTimeoutError` does);
        a transient one is then categorised ``UNKNOWN_OUTCOME``.
    """
    classified = _classify(
        exc,
        predicates=predicates,
        transient=transient,
        permanent=permanent,
        unknown_policy=unknown_policy,
        infrastructure=_infrastructure_errors if infrastructure is None else infrastructure,
    )
    if getattr(exc, "outcome_unknown", False) is True:
        return ClassifiedError(
            severity=classified.severity,
            original=exc,
            reason=f"{classified.reason}; outcome unknown (side effects may have happened)",
            outcome_unknown=True,
            category=classified.category,
        )
    return classified


def _result(exc: BaseException, category: ErrorCategory, reason: str) -> ClassifiedError:
    return ClassifiedError(
        severity=category.severity, original=exc, reason=f"{reason}: {type(exc).__name__}", category=category
    )


def _classify(
    exc: BaseException,
    *,
    predicates: Sequence[ErrorPredicate],
    transient: tuple[type[BaseException], ...],
    permanent: tuple[type[BaseException], ...],
    unknown_policy: ErrorSeverity,
    infrastructure: tuple[type[BaseException], ...],
) -> ClassifiedError:
    # 0. Shutdown is never a handler failure.
    if isinstance(exc, SHUTDOWN_ERRORS):
        return _result(exc, ErrorCategory.SHUTDOWN, "shutdown")

    # 1. A category the exception declares itself.
    declared = getattr(exc, ERROR_CATEGORY_ATTR, None)
    if isinstance(declared, ErrorCategory):
        return _result(exc, declared, f"declared {declared.value}")

    # 2. User predicates (first non-None wins)
    for predicate in predicates:
        result = predicate(exc)
        if result is True:
            return _result(exc, ErrorCategory.RETRYABLE, "predicate classified as transient")
        if result is False:
            return _result(exc, ErrorCategory.PERMANENT, "predicate classified as permanent")
        if isinstance(result, ErrorCategory):
            return _result(exc, result, f"predicate classified as {result.value}")

    # 3. Transport connection/channel errors (registered, see above)
    if infrastructure and isinstance(exc, infrastructure):
        return _result(exc, ErrorCategory.INFRASTRUCTURE, "infrastructure error")

    # 4. Transient tuple (isinstance check)
    if isinstance(exc, transient):
        return _result(exc, ErrorCategory.RETRYABLE, "transient error")

    # 5. Permanent tuple (isinstance check)
    if isinstance(exc, permanent):
        return _result(exc, ErrorCategory.PERMANENT, "permanent error")

    # 6. Unknown policy
    category = ErrorCategory.RETRYABLE if unknown_policy is ErrorSeverity.TRANSIENT else ErrorCategory.PERMANENT
    return _result(exc, category, f"unknown error classified as {unknown_policy.value}")


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


# ── Terminal connection failures (G-08) ──────────────────────────────────
# All subclass the builtin ConnectionError (an OSError): the client-library
# errors these replace (aiormq's ProbableAuthenticationError, ssl's
# SSLCertVerificationError) were OSErrors too, so ``except ConnectionError`` /
# ``except OSError`` around ``start()`` keeps working.


class TerminalConnectionError(ConnectionError):
    """rabbitkit stopped trying to (re)connect to RabbitMQ.

    Base class for the two reasons it gives up: a failure that retrying
    cannot fix (:class:`PermanentConnectionError`) and a spent reconnect
    budget (:class:`ReconnectBudgetExhaustedError`). Raised by
    ``AsyncBroker.start()``, reported to ``on_connection_failed`` hooks and
    re-raised by ``AsyncBroker.run()`` when it happens after startup.

    ``reason`` is a short, bounded snake_case code (safe as a metric label).
    ``endpoint`` is ``host:port`` of the node that failed. Neither the
    message nor any attribute ever carries credentials.
    """

    def __init__(self, message: str, *, reason: str, endpoint: str = "") -> None:
        super().__init__(message)
        self.reason = reason
        self.endpoint = endpoint


class PermanentConnectionError(TerminalConnectionError):
    """A connect attempt failed in a way that retrying will not fix.

    RabbitMQ refused the login, the vhost or the user's access to it, or TLS
    certificate verification failed. Retrying with the same settings only
    burns time (it used to retry for 6 to 12 minutes at startup and forever
    after a later drop), so rabbitkit fails fast instead. Fix the
    configuration, the credentials or the broker's permissions.
    """


class AuthenticationFailedError(PermanentConnectionError):
    """RabbitMQ refused the credentials (``ACCESS_REFUSED``, 403) or offered no
    SASL mechanism the client supports.

    ``reason`` is ``"authentication_failed"``.
    """


class VhostAccessRefusedError(PermanentConnectionError):
    """RabbitMQ refused ``connection.open``: the vhost does not exist or the
    user has no permissions on it (``NOT_ALLOWED``, 530).

    ``reason`` is ``"vhost_access_refused"``.
    """


class TLSVerificationError(PermanentConnectionError):
    """The TLS handshake failed for a reason retrying cannot fix: the broker's
    certificate did not verify (unknown CA, expired, hostname mismatch), the
    broker rejected the client certificate, or the port does not speak TLS.

    ``reason`` is ``"tls_verification_failed"`` or ``"tls_handshake_rejected"``.
    """


class ReconnectBudgetExhaustedError(TerminalConnectionError):
    """A connection was lost and every reconnect attempt failed until
    ``ConnectionConfig.reconnect_budget_attempts`` or
    ``reconnect_budget_seconds`` ran out.

    ``reason`` is ``"reconnect_budget_exhausted"``. The last attempt's error
    is the ``__cause__``.
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


class UnsafeToBrowseError(RuntimeError):
    """Peeking at or replaying from a queue could lose messages.

    Raised by :class:`~rabbitkit.dlq.DLQInspector` and the ``rabbitkit dlq``
    CLI for a quorum queue with a delivery limit: every requeue counts as a
    delivery, and past the limit RabbitMQ drops the message or dead-letters
    it away. Also raised when the limit can't be determined (fail closed).
    The message says how to make the queue safe to browse.
    """


class DLQOperationDeniedError(PermissionError):
    """A DLQ replay or purge was refused by the inspector's ``authorizer``
    (or the CLI's ``--require-actor``) before anything was read or
    published. The denial is written to the ``rabbitkit.dlq.audit`` logger.
    """


class BackpressureError(Exception):
    """Raised when publish-side flow control blocks a publish attempt.

    Only raised when ``BackpressureConfig.on_blocked == "raise"``.
    """
