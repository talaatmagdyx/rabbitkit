"""Transport ABCs and capability sub-protocols.

Defines the minimal interfaces that sync and async transports must implement,
plus opt-in capability protocols for publisher confirms, backpressure, RPC,
circuit breakers, and metrics.

ZERO pika or aio-pika imports — truly transport-agnostic.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any, Protocol, runtime_checkable

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import MessageEnvelope, PublishOutcome

# ── Core transport protocols ──────────────────────────────────────────────


@runtime_checkable
class Transport(Protocol):
    """Sync transport — implemented by sync/transport.py.

    Minimal interface for sync message broker I/O.
    """

    def connect(self) -> None:
        """Establish connection to broker."""
        ...

    def disconnect(self) -> None:
        """Close connection to broker."""
        ...

    def is_connected(self) -> bool:
        """Check if transport is connected."""
        ...

    def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        """Publish a message. Returns outcome with confirm status."""
        ...

    def consume(
        self,
        queue: str,
        callback: Callable[[RabbitMessage], None],
        prefetch: int = 10,
        *,
        no_ack: bool = False,
        declare: bool = True,
    ) -> str:
        """Start consuming from a queue. Returns consumer_tag.

        ``no_ack=True`` starts a no-ack consumer (the broker auto-acks on
        delivery; the built ``RabbitMessage`` gets no settlement functions).
        ``declare=False`` skips declaring/checking the queue first — required
        for AMQP pseudo-queues such as ``amq.rabbitmq.reply-to``, which the
        broker rejects any Queue.Declare for (active or passive).
        """
        ...

    def declare_exchange(self, exchange: RabbitExchange) -> None:
        """Declare an exchange on the broker."""
        ...

    def declare_queue(self, queue: RabbitQueue) -> None:
        """Declare a queue on the broker."""
        ...

    def bind_queue(self, queue: str, exchange: str, routing_key: str) -> None:
        """Bind a queue to an exchange with a routing key."""
        ...

    def bind_exchange(
        self,
        destination: str,
        source: str,
        routing_key: str = "",
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """Bind an exchange to another exchange (exchange-to-exchange binding)."""
        ...

    def cancel_consumer(self, consumer_tag: str) -> None:
        """Cancel a consumer by its tag."""
        ...


@runtime_checkable
class AsyncTransport(Protocol):
    """Async transport — implemented by async_/transport.py.

    Minimal interface for async message broker I/O.
    """

    async def connect(self) -> None:
        """Establish connection to broker."""
        ...

    async def disconnect(self) -> None:
        """Close connection to broker."""
        ...

    def is_connected(self) -> bool:
        """Check if transport is connected."""
        ...

    async def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        """Publish a message. Returns outcome with confirm status."""
        ...

    async def consume(
        self,
        queue: str,
        callback: Callable[[RabbitMessage], Awaitable[None]],
        prefetch: int = 10,
        *,
        no_ack: bool = False,
        declare: bool = True,
    ) -> str:
        """Start consuming from a queue. Returns consumer_tag.

        ``no_ack=True`` starts a no-ack consumer (the broker auto-acks on
        delivery; the built ``RabbitMessage`` gets no settlement functions).
        ``declare=False`` skips declaring/checking the queue first — required
        for AMQP pseudo-queues such as ``amq.rabbitmq.reply-to``, which the
        broker rejects any Queue.Declare for (active or passive).
        """
        ...

    async def declare_exchange(self, exchange: RabbitExchange) -> None:
        """Declare an exchange on the broker."""
        ...

    async def declare_queue(self, queue: RabbitQueue) -> None:
        """Declare a queue on the broker."""
        ...

    async def bind_queue(self, queue: str, exchange: str, routing_key: str) -> None:
        """Bind a queue to an exchange with a routing key."""
        ...

    async def bind_exchange(
        self,
        destination: str,
        source: str,
        routing_key: str = "",
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """Bind an exchange to another exchange (exchange-to-exchange binding)."""
        ...

    async def cancel_consumer(self, consumer_tag: str) -> None:
        """Cancel a consumer by its tag."""
        ...


# ── DLQ / inspection sub-protocols ────────────────────────────────────────


@runtime_checkable
class SupportsBasicGet(Protocol):
    """Transport supports basic.get (single-message fetch)."""

    def basic_get(self, queue: str) -> RabbitMessage | None:
        """Fetch a single message from a queue. Returns None if empty."""
        ...


@runtime_checkable
class AsyncSupportsBasicGet(Protocol):
    """Async transport supports basic.get."""

    async def basic_get(self, queue: str) -> RabbitMessage | None:
        """Fetch a single message from a queue. Returns None if empty."""
        ...


@runtime_checkable
class SupportsPurge(Protocol):
    """Transport supports queue purge."""

    def purge_queue(self, queue: str) -> int:
        """Purge all messages from a queue. Returns purged count."""
        ...


@runtime_checkable
class AsyncSupportsPurge(Protocol):
    """Async transport supports queue purge."""

    async def purge_queue(self, queue: str) -> int:
        """Purge all messages from a queue. Returns purged count."""
        ...


# ── Capability sub-protocols (opt-in) ─────────────────────────────────────


@runtime_checkable
class SupportsPublisherConfirms(Protocol):
    """Transport supports confirm_delivery mode."""

    def enable_confirms(self) -> None:
        """Enable publisher confirms on the channel."""
        ...


@runtime_checkable
class SupportsBackpressure(Protocol):
    """Transport supports connection.blocked/unblocked callbacks."""

    def on_blocked(self, callback: Callable[[], None]) -> None:
        """Register callback for connection.blocked."""
        ...

    def on_unblocked(self, callback: Callable[[], None]) -> None:
        """Register callback for connection.unblocked."""
        ...


@runtime_checkable
class SupportsRPC(Protocol):
    """Transport supports exclusive reply queues for RPC."""

    def create_reply_queue(self) -> str:
        """Create an exclusive reply queue. Returns queue name."""
        ...


# ── Generic extension protocols ──────────────────────────────────────────


@runtime_checkable
class CircuitBreakerProtocol(Protocol):
    """Generic circuit breaker protocol.

    obskit's CircuitBreaker and pybreaker both satisfy this interface.
    Used optionally by transports — core works without any CB.
    """

    def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        """Execute func through circuit breaker."""
        ...


@runtime_checkable
class AsyncCircuitBreakerProtocol(Protocol):
    """Async circuit breaker protocol.

    For async transports that need async circuit breaker support.
    """

    async def call_async(self, func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
        """Execute async func through circuit breaker."""
        ...


@runtime_checkable
class MetricsCollector(Protocol):
    """Optional metrics collector — obskit provides implementation.

    Used for observability integration. No-op when obskit is not installed.
    """

    def increment(self, name: str, tags: dict[str, str] | None = None) -> None:
        """Increment a counter metric."""
        ...

    def histogram(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Record a histogram metric value."""
        ...


@runtime_checkable
class AsyncMetricsCollector(Protocol):
    """Async metrics collector for async transports."""

    async def increment(self, name: str, tags: dict[str, str] | None = None) -> None:
        """Increment a counter metric."""
        ...

    async def histogram(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        """Record a histogram metric value."""
        ...


# ── Health sub-protocol ─────────────────────────────────────────────────


@runtime_checkable
class HealthProvider(Protocol):
    """Broker health surface — typed alternative to private-attribute probing.

    Brokers that expose these read-only properties satisfy the protocol and
    :mod:`rabbitkit.health` will use them directly. Brokers that still use
    private attributes (``_started``, ``_transport``, ...) are supported via
    the deprecation fallback in :func:`rabbitkit.health._get`.
    """

    @property
    def started(self) -> bool:
        """Whether the broker has been started."""
        ...

    @property
    def connected(self) -> bool:
        """Whether the underlying transport is connected."""
        ...

    @property
    def consumer_count(self) -> int:
        """Number of routes with an active (live) consumer."""
        ...

    @property
    def route_count(self) -> int:
        """Total number of registered routes."""
        ...

    @property
    def worker_pool_pending(self) -> int:
        """Current worker-pool backlog (pending tasks)."""
        ...

    @property
    def last_heartbeat(self) -> float | None:
        """Last liveness heartbeat (monotonic seconds), or None."""
        ...
