"""Route definition — internal route model.

Contains all metadata needed to:
- declare topology
- start consuming
- process messages through pipeline
- publish results

Produced by SubscriberRegistry, consumed by Broker.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from rabbitkit.core.config import RetryConfig, RetryDisabled, SafetyConfig
from rabbitkit.core.errors import ConfigurationError, UnsafeTopologyError
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import AckPolicy, ExchangeType, RejectWithoutDLXPolicy
from rabbitkit.serialization.base import Serializer

if TYPE_CHECKING:
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.protocols import Transport  # noqa: F401
    from rabbitkit.middleware.base import BaseMiddleware


# ── Result publisher ─────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class ResultPublisher:
    """Where to publish handler return values.

    Set by @publisher decorator on a handler.
    """

    exchange: RabbitExchange | str | None = None
    routing_key: str = ""

    def resolve_exchange_name(self) -> str:
        """Get the exchange name as string."""
        if self.exchange is None:
            return ""
        if isinstance(self.exchange, str):
            return self.exchange
        return self.exchange.name


# ── Route runtime state ──────────────────────────────────────────────────


@dataclass(slots=True)
class RouteRuntimeState:
    """Mutable per-route runtime state, held by the (frozen) RouteDefinition.

    Separated from registration metadata so that ``RouteDefinition`` can be
    immutable (frozen) while still allowing the broker to update runtime
    fields (``consumer_tag``) during start/reconnect.

    The frozen ``RouteDefinition`` holds a reference to this object; the
    dataclass is frozen (the field cannot be reassigned) but the inner
    object is mutable (``consumer_tag`` can be updated in place). This is
    the standard "frozen container holding mutable internals" pattern.
    """

    consumer_tag: str | None = None


# ── Route definition ─────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class RouteDefinition:
    """Internal route model — produced by registry, consumed by broker.

    Registration metadata is immutable (frozen) after creation. Runtime
    state (``consumer_tag``) lives in the separate mutable
    :class:`RouteRuntimeState` instance held by :attr:`runtime_state`, and
    is populated by the broker during start/reconnect.

    A backward-compatible ``consumer_tag`` property is provided (read-only)
    that delegates to ``runtime_state.consumer_tag`` so that callers that
    still read ``route.consumer_tag`` (e.g. ``health.py``) keep working
    without modification. Brokers write via
    ``route.runtime_state.consumer_tag = ...``.

    Contains all metadata needed to:
    - declare topology
    - start consuming
    - process messages through pipeline
    - publish results

    See Contracts 1, 4, 5 for semantic rules.
    """

    # ── Registration metadata (fixed after creation) ──

    # Identity
    name: str

    # Consumer side
    queue: RabbitQueue
    exchange: RabbitExchange | None
    handler: Callable[..., Any]
    ack_policy: AckPolicy = AckPolicy.AUTO
    route_middlewares: list[BaseMiddleware] = field(default_factory=list)

    # Publisher side (optional)
    result_publisher: ResultPublisher | None = None

    # Overrides
    serializer_override: Serializer[Any] | None = None
    retry_override: RetryConfig | RetryDisabled | None = None
    prefetch_count: int | None = None  # Per-route prefetch override (None=use global)
    tags: frozenset[str] = field(default_factory=frozenset)
    description: str = ""

    # Filter predicate — reject messages before deserialization
    filter_fn: Callable[[RabbitMessage], bool] | None = None

    # Per-route override of SafetyConfig.reject_without_dlx (None = inherit)
    reject_without_dlx: str | None = None

    # ── Runtime state (mutable sub-object; populated by broker) ──
    runtime_state: RouteRuntimeState = field(default_factory=RouteRuntimeState)

    # ── Backward-compatible runtime accessor (read-only) ──

    @property
    def consumer_tag(self) -> str | None:
        """Active consumer tag for this route (delegates to runtime_state).

        Read-only (L10) — kept for callers (e.g. ``health.py``) that read
        ``route.consumer_tag``. To set it, write
        ``route.runtime_state.consumer_tag = ...`` directly;
        ``route.consumer_tag = ...`` raises (a ``TypeError``, not a clean
        ``FrozenInstanceError`` — a side effect of ``frozen=True`` combined
        with ``slots=True`` and a same-named property) just like assigning
        any other field on this frozen dataclass. There is no special-cased
        ``__setattr__`` here — the mutable runtime state lives entirely in
        the separate :class:`RouteRuntimeState` object, and this class is
        genuinely frozen.
        """
        return self.runtime_state.consumer_tag

    def has_retry_enabled(self, broker_retry: RetryConfig | None = None) -> bool:
        """Check if this route has retry enabled.

        Resolution order:
        1. retry_override=RetryDisabled → NO retry (explicit opt-out)
        2. retry_override=RetryConfig(...) → YES retry (per-route override)
        3. retry_override=None → inherit broker default (broker_retry)
        """
        if isinstance(self.retry_override, RetryDisabled):
            return False
        if isinstance(self.retry_override, RetryConfig):
            return True
        # None → inherit broker default
        return broker_retry is not None

    def effective_retry_config(self, broker_retry: RetryConfig | None = None) -> RetryConfig | None:
        """Get the effective retry config for this route.

        Returns None if retry is disabled.
        """
        if isinstance(self.retry_override, RetryDisabled):
            return None
        if isinstance(self.retry_override, RetryConfig):
            return self.retry_override
        return broker_retry

    def validate_retry_ack_compatibility(self, broker_retry: RetryConfig | None = None) -> None:
        """Validate that retry + ack policy are compatible.

        Raises ConfigurationError if retry is enabled on MANUAL or ACK_FIRST routes.
        Called at registration time — fail fast.
        """
        if not self.has_retry_enabled(broker_retry):
            return

        if self.ack_policy == AckPolicy.MANUAL:
            raise ConfigurationError(
                f"Route '{self.name}': retry is incompatible with MANUAL ack policy. "
                "MANUAL mode means the handler owns settlement — retry cannot interfere. "
                "Either set ack_policy=AUTO or disable retry via retry=RETRY_DISABLED."
            )

        if self.ack_policy == AckPolicy.ACK_FIRST:
            raise ConfigurationError(
                f"Route '{self.name}': retry is incompatible with ACK_FIRST ack policy. "
                "ACK_FIRST acks before the handler runs — retry cannot nack/reject. "
                "Either set ack_policy=AUTO or disable retry via retry=RETRY_DISABLED."
            )

    def validate_retry_dlx_conflict(self, broker_retry: RetryConfig | None = None) -> None:
        """Validate that retry and manual DLX config don't conflict.

        If retry is enabled, RetryRouter owns DLQ topology.
        User must not also set dead_letter_exchange on the queue.
        """
        if not self.has_retry_enabled(broker_retry):
            return

        if self.queue.dead_letter_exchange is not None:
            raise ConfigurationError(
                f"Route '{self.name}': retry is enabled but queue '{self.queue.name}' "
                "already has dead_letter_exchange set. RetryRouter owns DLQ topology "
                "when retry is enabled — do not set dead_letter_exchange manually. "
                "To use custom DLQ routing, disable retry via retry=RETRY_DISABLED."
            )

        if self.queue.dead_letter_routing_key is not None:
            raise ConfigurationError(
                f"Route '{self.name}': retry is enabled but queue '{self.queue.name}' "
                "already has dead_letter_routing_key set. RetryRouter owns DLQ topology "
                "when retry is enabled — do not set dead_letter_routing_key manually. "
                "To use custom DLQ routing, disable retry via retry=RETRY_DISABLED."
            )

    def validate_headers_binding(self) -> None:
        """Validate headers-exchange bindings at registration time (C4).

        A headers exchange routes on binding *arguments*, not routing keys —
        an argument-less binding matches everything (silent firehose /
        misrouting). Require ``bind_arguments`` with a valid ``x-match``.
        """
        if self.exchange is None or self.exchange.type != ExchangeType.HEADERS:
            return
        if not self.queue.bind_arguments:
            raise ConfigurationError(
                f"Route '{self.name}': queue '{self.queue.name}' binds to headers "
                f"exchange '{self.exchange.name}' without bind_arguments. A headers "
                "binding with no arguments matches EVERY message. Set "
                "RabbitQueue(bind_arguments={'x-match': 'all'|'any', ...})."
            )
        x_match = self.queue.bind_arguments.get("x-match", "all")
        if x_match not in ("all", "any", "all-with-x", "any-with-x"):
            raise ConfigurationError(
                f"Route '{self.name}': bind_arguments['x-match'] must be 'all', "
                f"'any', 'all-with-x', or 'any-with-x'; got {x_match!r}."
            )

    def validate_reject_without_dlx_value(self) -> None:
        """Validate the per-route reject_without_dlx override value."""
        if self.reject_without_dlx is None:
            return
        if self.reject_without_dlx not in ("auto_provision", "error", "discard"):
            raise ConfigurationError(
                f"Route '{self.name}': reject_without_dlx must be one of "
                f"'auto_provision', 'error', 'discard'; got {self.reject_without_dlx!r}."
            )

    def validate(self, broker_retry: RetryConfig | None = None) -> None:
        """Run all route validations.

        Called at registration time — fail fast on conflicts.
        """
        self.validate_retry_ack_compatibility(broker_retry)
        self.validate_retry_dlx_conflict(broker_retry)
        self.validate_headers_binding()
        self.validate_reject_without_dlx_value()

    def can_reject_without_dlx(self, broker_retry: RetryConfig | None = None) -> bool:
        """True when this route can ``reject(requeue=False)`` into a queue
        with no dead-letter path — i.e. RabbitMQ would discard the message.

        Retry-enabled routes get a DLX from RetryRouter; a manually
        configured ``dead_letter_exchange`` is a dead-letter path already.
        ACK_FIRST routes without a filter ack before the handler runs, so
        they never reject. Everything else (AUTO/NACK_ON_ERROR/MANUAL,
        filter_fn, permanent-error classification) can reject.
        """
        if self.effective_retry_config(broker_retry) is not None:
            return False
        if self.queue.dead_letter_exchange is not None:
            return False
        return not (self.ack_policy == AckPolicy.ACK_FIRST and self.filter_fn is None)

    def resolve_safety_dlq(
        self,
        safety: SafetyConfig,
        broker_retry: RetryConfig | None = None,
    ) -> str | None:
        """Apply the reject-without-DLX safety policy to this route (C3).

        Returns the DLQ name to auto-provision, or ``None`` when nothing is
        needed (route already has a dead-letter path, cannot reject, or the
        policy is ``discard``). Raises :class:`UnsafeTopologyError` under the
        ``error`` policy.

        Called by brokers during topology declaration, only under
        ``TopologyMode.AUTO_DECLARE``.
        """
        if not self.can_reject_without_dlx(broker_retry):
            return None

        policy = self.reject_without_dlx or safety.reject_without_dlx
        if policy == RejectWithoutDLXPolicy.AUTO_PROVISION:
            return f"{self.queue.name}{safety.dlq_suffix}"
        if policy == RejectWithoutDLXPolicy.ERROR:
            raise UnsafeTopologyError(
                f"Queue '{self.queue.name}' (route '{self.name}') can reject "
                "messages with requeue=False, but no dead-letter exchange is "
                "configured — RabbitMQ would discard them permanently. Configure "
                "a DLX/DLQ on the queue, use reject_without_dlx='auto_provision', "
                "or explicitly opt into loss with reject_without_dlx='discard'."
            )
        # policy == DISCARD: explicit opt-in to loss
        if safety.warn_on_discard:
            import warnings

            warnings.warn(
                f"Route {self.name!r}: reject_without_dlx='discard' — messages "
                f"rejected with requeue=False on queue {self.queue.name!r} will be "
                "permanently discarded by RabbitMQ (no dead-letter exchange). Use "
                "this only when message loss is acceptable.",
                RuntimeWarning,
                stacklevel=2,
            )
        return None


# ``ConfigurationError`` now lives in ``rabbitkit.core.errors`` (single
# canonical location). It is re-exported here for backwards compatibility with
# code that imported it from this module.
__all__ = ["ConfigurationError", "ResultPublisher", "RouteDefinition", "RouteRuntimeState"]
