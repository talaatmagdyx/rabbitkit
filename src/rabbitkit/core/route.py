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

from rabbitkit.core.config import RetryConfig, RetryDisabled
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import AckPolicy

if TYPE_CHECKING:
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.protocols import Transport  # noqa: F401


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


# ── Route definition ─────────────────────────────────────────────────────


@dataclass(slots=True)
class RouteDefinition:
    """Internal route model — produced by registry, consumed by broker.

    Registration metadata is fixed after creation; runtime fields
    (consumer_tag) are populated by the broker during start/reconnect.

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
    route_middlewares: list[Any] = field(default_factory=list)

    # Publisher side (optional)
    result_publisher: ResultPublisher | None = None

    # Overrides
    serializer_override: Any | None = None  # Serializer protocol, typed later
    retry_override: RetryConfig | RetryDisabled | None = None
    prefetch_count: int | None = None  # Per-route prefetch override (None=use global)
    tags: frozenset[str] = field(default_factory=frozenset)
    description: str = ""

    # Filter predicate — reject messages before deserialization
    filter_fn: Callable[[RabbitMessage], bool] | None = None

    # ── Runtime state (populated by broker, updated on reconnect) ──
    consumer_tag: str | None = None

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

    def validate(self, broker_retry: RetryConfig | None = None) -> None:
        """Run all route validations.

        Called at registration time — fail fast on conflicts.
        """
        self.validate_retry_ack_compatibility(broker_retry)
        self.validate_retry_dlx_conflict(broker_retry)


class ConfigurationError(Exception):
    """Raised for invalid configuration combinations detected at registration time."""
