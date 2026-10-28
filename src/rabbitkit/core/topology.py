"""Exchange & Queue models with validation and declaration builders.

All topology validation happens here. Transport adapters call
to_declare_kwargs() / to_bind_kwargs() to get the appropriate
keyword arguments for pika or aio-pika declaration calls.
"""

from __future__ import annotations

import re
import warnings
from dataclasses import dataclass, field
from typing import Any

from rabbitkit.core.errors import TopologyValidationError
from rabbitkit.core.types import DeadLetterStrategy, ExchangeType, QueueType, validate_amqp_shortstr


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
        # Accept the enum's string value (``type="topic"``): ``to_declare_kwargs``
        # needs the enum, and a plain string used to fail at startup with
        # ``AttributeError: 'str' object has no attribute 'value'``.
        if not isinstance(self.type, ExchangeType):
            try:
                object.__setattr__(self, "type", ExchangeType(self.type))
            except ValueError:
                raise TopologyValidationError(
                    f"Exchange {self.name!r}: unknown type {self.type!r}; expected one of "
                    f"{[t.value for t in ExchangeType]}"
                ) from None
        self.validate()

    def validate(self) -> None:
        """Validate exchange configuration."""
        if not self.name and self.type != ExchangeType.DIRECT:
            msg = "Non-default exchanges must have a name"
            raise TopologyValidationError(msg)
        if self.internal and self.auto_delete:
            msg = "Internal exchanges cannot be auto_delete (they are never published to directly)."
            raise TopologyValidationError(msg)
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
    # x-single-active-consumer — NOT quorum-specific: valid on classic
    # (RabbitMQ 3.8+) and quorum queues alike. The standard active/standby
    # pattern: N replicas subscribe, the broker delivers to exactly one and
    # fails over automatically when it dies.
    single_active_consumer: bool = False
    # x-quorum-initial-group-size — replica count at declaration (quorum only;
    # the broker default is min(cluster size, 3)). Creation-time only.
    quorum_initial_group_size: int | None = None
    # x-dead-letter-strategy (quorum only, RabbitMQ 3.10+): "at-least-once"
    # keeps a dead-lettered message on THIS queue until the dead-letter
    # target confirms it (instead of dropping it when the DLQ is missing or
    # full). Requires overflow="reject-publish".
    dead_letter_strategy: DeadLetterStrategy | None = None

    # Overflow
    overflow: str | None = None  # "drop-head" | "reject-publish" | "reject-publish-dlx"

    # Expiry
    expires: int | None = None  # ms — auto-delete after idle

    # Extra arguments (escape hatch)
    arguments: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.dead_letter_strategy is not None and not isinstance(self.dead_letter_strategy, DeadLetterStrategy):
            try:
                object.__setattr__(self, "dead_letter_strategy", DeadLetterStrategy(self.dead_letter_strategy))
            except ValueError:
                raise TopologyValidationError(
                    f"Queue {self.name!r}: unknown dead_letter_strategy {self.dead_letter_strategy!r}; "
                    f"expected one of {[s.value for s in DeadLetterStrategy]}"
                ) from None
        if not isinstance(self.queue_type, QueueType):
            try:
                object.__setattr__(self, "queue_type", QueueType(self.queue_type))
            except ValueError:
                raise TopologyValidationError(
                    f"Queue {self.name!r}: unknown queue_type {self.queue_type!r}; expected one of "
                    f"{[t.value for t in QueueType]}"
                ) from None
        self.validate()

    def validate(self) -> None:
        """Enforce queue-type-specific constraints.

        Raises TopologyValidationError (a ValueError subclass) for
        invalid combinations.
        Uses warnings.warn() for unusual but legal combinations.
        """
        if not self.name:
            msg = "Queue name is required"
            raise TopologyValidationError(msg)
        validate_amqp_shortstr("Queue name", self.name)
        validate_amqp_shortstr("Queue routing_key", self.routing_key)

        # Quorum constraints
        if self.queue_type == QueueType.QUORUM:
            if not self.durable:
                msg = "Quorum queues must be durable"
                raise TopologyValidationError(msg)
            if self.exclusive:
                msg = "Quorum queues cannot be exclusive"
                raise TopologyValidationError(msg)
            if self.lazy:
                msg = "Quorum queues do not support lazy mode (x-queue-mode)"
                raise TopologyValidationError(msg)
            if self.max_priority is not None:
                msg = "Quorum queues do not support priorities"
                raise TopologyValidationError(msg)

        # Stream constraints
        if self.queue_type == QueueType.STREAM:
            if not self.durable:
                msg = "Stream queues must be durable"
                raise TopologyValidationError(msg)
            if self.exclusive:
                msg = "Stream queues cannot be exclusive"
                raise TopologyValidationError(msg)
            if self.lazy:
                msg = "Stream queues do not support lazy mode"
                raise TopologyValidationError(msg)
            if self.max_priority is not None:
                msg = "Stream queues do not support priorities"
                raise TopologyValidationError(msg)
            if self.message_ttl is not None:
                msg = "Stream queues do not support message TTL"
                raise TopologyValidationError(msg)
            if self.consumer_timeout is not None:
                msg = "Stream queues do not support consumer_timeout (per-message ack timeouts do not apply to streams)"
                raise TopologyValidationError(msg)

        if self.consumer_timeout is not None and self.consumer_timeout <= 0:
            msg = f"Queue '{self.name}': consumer_timeout must be a positive number of milliseconds"
            raise TopologyValidationError(msg)

        # Classic constraints
        if self.queue_type == QueueType.CLASSIC:
            if self.delivery_limit is not None:
                msg = "Classic queues do not support delivery_limit (quorum only)"
                raise TopologyValidationError(msg)

        # Quorum-only options
        if self.queue_type != QueueType.QUORUM:
            if self.quorum_initial_group_size is not None:
                msg = f"Queue '{self.name}': quorum_initial_group_size is only valid for quorum queues"
                raise TopologyValidationError(msg)
            if self.dead_letter_strategy is not None:
                msg = f"Queue '{self.name}': dead_letter_strategy is only valid for quorum queues"
                raise TopologyValidationError(msg)
        if self.quorum_initial_group_size is not None and self.quorum_initial_group_size < 1:
            msg = f"Queue '{self.name}': quorum_initial_group_size must be >= 1"
            raise TopologyValidationError(msg)
        if (
            self.dead_letter_strategy is None
            and self.arguments.get("x-dead-letter-strategy") == DeadLetterStrategy.AT_LEAST_ONCE.value
            and self.arguments.get("x-overflow", self.overflow) != "reject-publish"
        ):
            warnings.warn(
                f"Queue '{self.name}': x-dead-letter-strategy=at-least-once needs "
                "overflow='reject-publish'; RabbitMQ falls back to at-most-once without it",
                UserWarning,
                stacklevel=3,
            )
        if self.dead_letter_strategy is DeadLetterStrategy.AT_LEAST_ONCE:
            overflow = self.arguments.get("x-overflow", self.overflow)
            if overflow != "reject-publish":
                msg = (
                    f"Queue '{self.name}': dead_letter_strategy='at-least-once' requires "
                    f"overflow='reject-publish' (got {overflow!r}); RabbitMQ silently falls back to "
                    "at-most-once otherwise"
                )
                raise TopologyValidationError(msg)

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
                self.quorum_initial_group_size is not None,
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
        if self.quorum_initial_group_size is not None:
            args["x-quorum-initial-group-size"] = self.quorum_initial_group_size
        if self.dead_letter_strategy is not None:
            args["x-dead-letter-strategy"] = self.dead_letter_strategy.value

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


# ── Declaration conflicts (406) ──────────────────────────────────────────

_INEQUIVALENT_ARG = re.compile(r"inequivalent arg '([^']+)'")
_RECEIVED_CURRENT = re.compile(r"received (?:the value )?(.*?) but current is (?:the value )?(.*)$")

_DLX_ARGS = frozenset({"x-dead-letter-exchange", "x-dead-letter-routing-key"})


def describe_declare_conflict(kind: str, name: str, broker_text: str) -> str:
    """An actionable message for a 406 ``PRECONDITION_FAILED`` on declare.

    Names the entity and the conflicting argument (parsed from the broker's
    reply), says why rabbitkit declared that argument, and gives the fixes —
    including treating the entity as infrastructure-managed
    (``TopologyMode.PASSIVE_ONLY``), where rabbitkit performs no writes.
    """
    arg_match = _INEQUIVALENT_ARG.search(broker_text)
    arg = arg_match.group(1) if arg_match else None
    values = _RECEIVED_CURRENT.search(broker_text)
    parts = [f"Cannot declare {kind} {name!r}: it already exists with incompatible arguments"]
    if arg is not None:
        detail = f"conflicting argument {arg!r}"
        if values is not None:
            declared, existing = values.group(1).strip(), values.group(2).strip()
            detail += f" (rabbitkit declared {declared}, the existing {kind} has {existing})"
        parts.append(detail)
    parts.append(f"broker said: {broker_text}")
    msg = "; ".join(parts) + "."
    if arg in _DLX_ARGS:
        msg += (
            f" rabbitkit adds {sorted(_DLX_ARGS)} to a consumed queue so terminal rejects reach "
            f"'{name}.dlq' instead of being discarded: retry (RetryConfig) or the safety DLX "
            "(SafetyConfig.reject_without_dlx='auto_provision', the default) asked for it."
        )
    elif arg == "x-queue-type":
        msg += (
            " The queue type differs (e.g. quorum vs classic); a queue's type cannot be changed "
            "in place — migrate it (see docs/quorum-migration.md) or set queue_type to match."
        )
    msg += (
        f" Fix: reconcile or migrate the existing {kind}; or make rabbitkit's definition match; "
        f"or, if this {kind} is owned by infrastructure, use TopologyMode.PASSIVE_ONLY — rabbitkit "
        "then performs no writes; check compatibility with "
        "broker.verify_topology(management_client=...)."
    )
    return msg
