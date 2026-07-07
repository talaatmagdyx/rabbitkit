"""Exchange & Queue models with validation and declaration builders.

All topology validation happens here. Transport adapters call
to_declare_kwargs() / to_bind_kwargs() to get the appropriate
keyword arguments for pika or aio-pika declaration calls.
"""

from __future__ import annotations

import warnings
from dataclasses import dataclass, field
from typing import Any

from rabbitkit.core.types import ExchangeType, QueueType, validate_amqp_shortstr


@dataclass(frozen=True, slots=True)
class RabbitExchange:
    """Exchange declaration model."""

    name: str
    type: ExchangeType = ExchangeType.DIRECT
    durable: bool = True
    auto_delete: bool = False
    passive: bool = False
    internal: bool = False
    arguments: dict[str, Any] = field(default_factory=dict)
    bind_to: str | None = None
    bind_arguments: dict[str, Any] = field(default_factory=dict)
    routing_key: str = ""

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Validate exchange configuration."""
        if not self.name and self.type != ExchangeType.DIRECT:
            msg = "Non-default exchanges must have a name"
            raise ValueError(msg)
        if self.internal and self.auto_delete:
            msg = "Internal exchanges cannot be auto_delete (they are never published to directly)."
            raise ValueError(msg)
        validate_amqp_shortstr("Exchange name", self.name)
        validate_amqp_shortstr("Exchange routing_key", self.routing_key)

    def to_declare_kwargs(self) -> dict[str, Any]:
        """Build exchange_declare kwargs for pika/aio-pika."""
        return {
            "exchange": self.name,
            "exchange_type": self.type.value,
            "durable": self.durable,
            "auto_delete": self.auto_delete,
            "passive": self.passive,
            "internal": self.internal,
            "arguments": self.arguments or None,
        }

    def to_bind_kwargs(self) -> dict[str, Any] | None:
        """Build exchange_bind kwargs. Returns None if no binding."""
        if self.bind_to is None:
            return None
        return {
            "destination": self.name,
            "source": self.bind_to,
            "routing_key": self.routing_key,
            "arguments": self.bind_arguments or None,
        }


@dataclass(frozen=True, slots=True)
class RabbitQueue:
    """Queue declaration model with type-specific validation."""

    name: str
    durable: bool = True
    exclusive: bool = False
    passive: bool = False
    auto_delete: bool = False
    routing_key: str = ""
    bind_arguments: dict[str, Any] = field(default_factory=dict)
    queue_type: QueueType = QueueType.CLASSIC

    # DLQ
    dead_letter_exchange: str | None = None
    dead_letter_routing_key: str | None = None

    # Limits
    message_ttl: int | None = None  # ms
    max_length: int | None = None
    max_length_bytes: int | None = None
    # x-consumer-timeout (ms) — per-queue override of the server's
    # consumer ack timeout (server default: 30 minutes). If a delivered
    # message stays unacked past it, RabbitMQ force-closes the consumer's
    # channel. The server does not advertise its limit to clients, so if a
    # handler can legitimately hold a message longer than 30 minutes, raise
    # it HERE at declaration time (RabbitMQ >= 3.12; classic/quorum only).
    consumer_timeout: int | None = None

    # Classic-only
    lazy: bool = False  # x-queue-mode: lazy (classic only)
    max_priority: int | None = None  # classic only (0-255)

    # Quorum-specific
    delivery_limit: int | None = None  # x-delivery-limit (quorum only)
    single_active_consumer: bool = False  # x-single-active-consumer

    # Overflow
    overflow: str | None = None  # "drop-head" | "reject-publish" | "reject-publish-dlx"

    # Expiry
    expires: int | None = None  # ms — auto-delete after idle

    # Extra arguments (escape hatch)
    arguments: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        self.validate()

    def validate(self) -> None:
        """Enforce queue-type-specific constraints.

        Raises ValueError for invalid combinations.
        Uses warnings.warn() for unusual but legal combinations.
        """
        if not self.name:
            msg = "Queue name is required"
            raise ValueError(msg)
        validate_amqp_shortstr("Queue name", self.name)
        validate_amqp_shortstr("Queue routing_key", self.routing_key)

        # Quorum constraints
        if self.queue_type == QueueType.QUORUM:
            if not self.durable:
                msg = "Quorum queues must be durable"
                raise ValueError(msg)
            if self.exclusive:
                msg = "Quorum queues cannot be exclusive"
                raise ValueError(msg)
            if self.lazy:
                msg = "Quorum queues do not support lazy mode (x-queue-mode)"
                raise ValueError(msg)
            if self.max_priority is not None:
                msg = "Quorum queues do not support priorities"
                raise ValueError(msg)

        # Stream constraints
        if self.queue_type == QueueType.STREAM:
            if not self.durable:
                msg = "Stream queues must be durable"
                raise ValueError(msg)
            if self.exclusive:
                msg = "Stream queues cannot be exclusive"
                raise ValueError(msg)
            if self.lazy:
                msg = "Stream queues do not support lazy mode"
                raise ValueError(msg)
            if self.max_priority is not None:
                msg = "Stream queues do not support priorities"
                raise ValueError(msg)
            if self.message_ttl is not None:
                msg = "Stream queues do not support message TTL"
                raise ValueError(msg)
            if self.consumer_timeout is not None:
                msg = "Stream queues do not support consumer_timeout (per-message ack timeouts do not apply to streams)"
                raise ValueError(msg)

        if self.consumer_timeout is not None and self.consumer_timeout <= 0:
            msg = f"Queue '{self.name}': consumer_timeout must be a positive number of milliseconds"
            raise ValueError(msg)

        # Classic constraints
        if self.queue_type == QueueType.CLASSIC:
            if self.delivery_limit is not None:
                msg = "Classic queues do not support delivery_limit (quorum only)"
                raise ValueError(msg)

        # Warnings for unusual combos
        if self.lazy:
            warnings.warn(
                f"Queue '{self.name}': lazy=True sets the deprecated x-queue-mode=lazy "
                "argument. RabbitMQ >=3.12 defaults classic queues to CQv2, which already "
                "keeps message bodies out of memory in a lazy-like manner -- x-queue-mode "
                "is a silent no-op there. On RabbitMQ <3.12 (or a classic queue explicitly "
                "downgraded to v1) it still has effect. If you're targeting >=3.12, drop "
                "lazy=True; the default queue behavior already covers this.",
                UserWarning,
                stacklevel=2,
            )

        if self.auto_delete and self.durable:
            warnings.warn(
                f"Queue '{self.name}': auto_delete=True with durable=True is unusual — "
                "the queue will be deleted when the last consumer disconnects, "
                "despite being durable",
                UserWarning,
                stacklevel=2,
            )

        if self.passive and any(
            [
                self.lazy,
                self.max_priority is not None,
                self.delivery_limit is not None,
                self.message_ttl is not None,
                self.max_length is not None,
                self.consumer_timeout is not None,
            ]
        ):
            warnings.warn(
                f"Queue '{self.name}': passive=True with creation-only options set — "
                "these options are ignored for passive declarations",
                UserWarning,
                stacklevel=2,
            )

    def to_declare_kwargs(self) -> dict[str, Any]:
        """Build queue_declare kwargs with merged x-arguments."""
        args: dict[str, Any] = {}

        # Queue type
        args["x-queue-type"] = self.queue_type.value

        # DLQ
        if self.dead_letter_exchange is not None:
            args["x-dead-letter-exchange"] = self.dead_letter_exchange
        if self.dead_letter_routing_key is not None:
            args["x-dead-letter-routing-key"] = self.dead_letter_routing_key

        # Limits
        if self.message_ttl is not None:
            args["x-message-ttl"] = self.message_ttl
        if self.max_length is not None:
            args["x-max-length"] = self.max_length
        if self.max_length_bytes is not None:
            args["x-max-length-bytes"] = self.max_length_bytes
        if self.consumer_timeout is not None:
            args["x-consumer-timeout"] = self.consumer_timeout

        # Classic-only
        if self.lazy:
            args["x-queue-mode"] = "lazy"
        if self.max_priority is not None:
            args["x-max-priority"] = self.max_priority

        # Quorum-specific
        if self.delivery_limit is not None:
            args["x-delivery-limit"] = self.delivery_limit
        if self.single_active_consumer:
            args["x-single-active-consumer"] = True

        # Overflow
        if self.overflow is not None:
            args["x-overflow"] = self.overflow

        # Expiry
        if self.expires is not None:
            args["x-expires"] = self.expires

        # Merge user-provided arguments (escape hatch takes precedence)
        args.update(self.arguments)

        return {
            "queue": self.name,
            "durable": self.durable,
            "exclusive": self.exclusive,
            "auto_delete": self.auto_delete,
            "passive": self.passive,
            "arguments": args,
        }

    def to_bind_kwargs(self, exchange: str) -> dict[str, Any]:
        """Build queue_bind kwargs."""
        return {
            "queue": self.name,
            "exchange": exchange,
            "routing_key": self.routing_key,
            "arguments": self.bind_arguments or None,
        }
