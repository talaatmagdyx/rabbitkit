"""AsyncTransport — aio-pika transport adapter.

Uses aio_pika.connect_robust() for auto-reconnection.
aio-pika owns connection/channel/consumer restoration natively.

Architecture:
- Publisher connection: dedicated connection + AsyncChannelPool for concurrent
  publishes without blocking consumer channels or topology operations.
- Consumer connection: dedicated connection for all subscribe operations.
  Each queue gets its own channel (required for per-queue QoS).
- Topology connection: reuses consumer connection for declare/bind operations.
"""

from __future__ import annotations

import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

from rabbitkit.async_.pool import AsyncConnectionPool
from rabbitkit.core.config import ConnectionConfig, PoolConfig, SecurityConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import (
    MessageEnvelope,
    PublishOutcome,
    PublishStatus,
    TopologyMode,
)

logger = logging.getLogger(__name__)


class AsyncTransportImpl:
    """aio-pika-based async transport adapter.

    Uses connect_robust() for transparent auto-reconnect.
    Publisher and consumer traffic run on separate connections to prevent
    head-of-line blocking and QoS interference.
    """

    def __init__(
        self,
        connection_config: ConnectionConfig | None = None,
        security_config: SecurityConfig | None = None,
        pool_config: PoolConfig | None = None,
        topology_mode: TopologyMode = TopologyMode.AUTO_DECLARE,
        confirm_delivery: bool = True,
    ) -> None:
        self._connection_config = connection_config or ConnectionConfig()
        self._security_config = security_config or SecurityConfig()
        self._pool_config = pool_config or PoolConfig()
        self._topology_mode = topology_mode
        self._confirm_delivery = confirm_delivery

        self._conn_pool = AsyncConnectionPool(
            self._connection_config,
            self._security_config,
            self._pool_config,
            publisher_confirms=self._confirm_delivery,
        )
        self._connected = False

        # Per-queue consumer channels: queue_name → aio_pika channel
        self._consumer_channels: dict[str, Any] = {}
        self._consumer_tags: dict[str, str] = {}  # queue_name → consumer_tag

        # Shared topology channel (consumer connection)
        self._topology_channel: Any | None = None

    async def connect(self) -> None:
        """Establish publisher and consumer connections."""
        if self._connected:
            return

        await self._conn_pool.connect()

        # Open topology channel on consumer connection
        consumer_conn = await self._conn_pool.get_consumer_connection()
        self._topology_channel = await consumer_conn.channel()

        self._connected = True
        logger.info(
            "Connected to RabbitMQ at %s:%d (async, pooled)",
            self._connection_config.host,
            self._connection_config.port,
        )

    async def disconnect(self) -> None:
        """Close all channels and connections."""
        if not self._connected:
            return

        try:
            # Close per-queue consumer channels
            for ch in list(self._consumer_channels.values()):
                try:
                    if not ch.is_closed:
                        await ch.close()
                except Exception:
                    pass
            self._consumer_channels.clear()
            self._consumer_tags.clear()

            # Close topology channel
            if self._topology_channel is not None and not self._topology_channel.is_closed:
                try:
                    await self._topology_channel.close()
                except Exception:
                    pass
            self._topology_channel = None

            await self._conn_pool.close_all()
        except Exception as e:
            logger.warning("Error during disconnect: %s", e)
        finally:
            self._connected = False
            logger.info("Disconnected from RabbitMQ (async)")

    def is_connected(self) -> bool:
        """Check if connected to RabbitMQ."""
        return self._connected

    async def _ensure_connected(self) -> None:
        """Ensure connection is established."""
        if self._connected:
            return
        await self.connect()

    async def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        """Publish a message using a channel from the publisher pool.

        Hot path: inlined channel acquire/release (no @asynccontextmanager) and a
        synchronous connected-check, to cut per-publish coroutine overhead. Same
        acquire -> publish -> release -> confirm semantics.
        """
        try:
            import aio_pika

            if not self._connected:
                await self._ensure_connected()

            channel = await self._conn_pool.acquire_publisher_channel()
            try:
                message = aio_pika.Message(
                    body=envelope.body,
                    message_id=envelope.message_id,
                    correlation_id=envelope.correlation_id,
                    reply_to=envelope.reply_to,
                    content_type=envelope.content_type,
                    content_encoding=envelope.content_encoding,
                    headers=envelope.headers or None,
                    delivery_mode=aio_pika.DeliveryMode(envelope.delivery_mode),
                    priority=envelope.priority,
                    expiration=(int(envelope.expiration) * 1000 if envelope.expiration else None),
                    type=envelope.type,
                    user_id=envelope.user_id,
                    app_id=envelope.app_id,
                )

                if envelope.exchange:
                    exchange = await channel.get_exchange(envelope.exchange, ensure=False)
                else:
                    exchange = channel.default_exchange

                await exchange.publish(
                    message,
                    routing_key=envelope.routing_key,
                    mandatory=envelope.mandatory,
                )
            finally:
                await self._conn_pool.release_publisher_channel(channel)

            return PublishOutcome(
                status=PublishStatus.CONFIRMED,
                exchange=envelope.exchange,
                routing_key=envelope.routing_key,
            )

        except Exception as e:
            logger.error("Async publish failed: %s", e)
            return PublishOutcome(
                status=PublishStatus.ERROR,
                exchange=envelope.exchange,
                routing_key=envelope.routing_key,
                error=e,
            )

    async def consume(
        self,
        queue: str,
        callback: Callable[[RabbitMessage], Awaitable[None]],
        prefetch: int = 10,
    ) -> str:
        """Start consuming from a queue.

        Each queue gets a dedicated channel so per-queue QoS settings do not
        interfere with each other.  Returns the consumer tag.
        """
        await self._ensure_connected()

        # Dedicated channel per consumer queue for isolated QoS
        consumer_conn = await self._conn_pool.get_consumer_connection()
        channel = await consumer_conn.channel()
        await channel.set_qos(prefetch_count=prefetch)
        self._consumer_channels[queue] = channel

        # passive declare (not get_queue): RobustChannel only restores queues in
        # its _queues registry, which declare_queue populates and get_queue does
        # not. Without this, the consumer is silently NOT resumed after a
        # connect_robust reconnect (the queue — and its consumer — are untracked).
        q = await channel.declare_queue(queue, passive=True)
        consumer_tag = f"rabbitkit.{uuid.uuid4()}"

        async def on_message(message: Any) -> None:
            rabbit_msg = self._build_message(message)
            await callback(rabbit_msg)

        await q.consume(on_message, consumer_tag=consumer_tag)
        self._consumer_tags[queue] = consumer_tag
        logger.info("Started consuming from queue '%s' with tag '%s' (async)", queue, consumer_tag)
        return consumer_tag

    async def declare_exchange(self, exchange: RabbitExchange) -> None:
        """Declare an exchange on the topology channel."""
        if self._topology_mode == TopologyMode.MANUAL:
            return

        await self._ensure_connected()
        assert self._topology_channel is not None

        kwargs = exchange.to_declare_kwargs()

        if self._topology_mode == TopologyMode.PASSIVE_ONLY or exchange.passive:
            await self._topology_channel.get_exchange(kwargs["exchange"], ensure=True)
        else:
            await self._topology_channel.declare_exchange(
                name=kwargs["exchange"],
                type=kwargs.get("exchange_type", "direct"),
                durable=kwargs.get("durable", True),
                auto_delete=kwargs.get("auto_delete", False),
                internal=kwargs.get("internal", False),
                arguments=kwargs.get("arguments"),
            )

    async def declare_queue(self, queue: RabbitQueue) -> None:
        """Declare a queue on the topology channel."""
        if self._topology_mode == TopologyMode.MANUAL:
            return

        await self._ensure_connected()
        assert self._topology_channel is not None

        kwargs = queue.to_declare_kwargs()

        if self._topology_mode == TopologyMode.PASSIVE_ONLY or queue.passive:
            await self._topology_channel.get_queue(kwargs["queue"], ensure=True)
        else:
            await self._topology_channel.declare_queue(
                name=kwargs["queue"],
                durable=kwargs.get("durable", True),
                exclusive=kwargs.get("exclusive", False),
                auto_delete=kwargs.get("auto_delete", False),
                arguments=kwargs.get("arguments"),
            )

    async def bind_queue(self, queue: str, exchange: str, routing_key: str) -> None:
        """Bind a queue to an exchange on the topology channel."""
        if self._topology_mode == TopologyMode.MANUAL:
            return

        await self._ensure_connected()
        assert self._topology_channel is not None

        q = await self._topology_channel.get_queue(queue, ensure=False)
        ex = await self._topology_channel.get_exchange(exchange, ensure=False)
        await q.bind(ex, routing_key=routing_key)

    async def bind_exchange(
        self,
        destination: str,
        source: str,
        routing_key: str = "",
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """Bind an exchange to another exchange on the topology channel."""
        if self._topology_mode == TopologyMode.MANUAL:
            return

        await self._ensure_connected()
        assert self._topology_channel is not None

        dest_ex = await self._topology_channel.get_exchange(destination, ensure=False)
        src_ex = await self._topology_channel.get_exchange(source, ensure=False)
        await dest_ex.bind(src_ex, routing_key=routing_key, arguments=arguments)

    async def cancel_consumer(self, consumer_tag: str) -> None:
        """Cancel a consumer by tag."""
        if not self._connected:
            return

        for queue_name, tag in list(self._consumer_tags.items()):
            if tag == consumer_tag:
                channel = self._consumer_channels.get(queue_name)
                if channel is not None:
                    try:
                        q = await channel.get_queue(queue_name, ensure=False)
                        await q.cancel(consumer_tag)
                    except Exception as e:
                        logger.warning("Failed to cancel consumer %s: %s", consumer_tag, e)
                    finally:
                        del self._consumer_tags[queue_name]
                        del self._consumer_channels[queue_name]
                break

    # ── DLQ / inspection (DLQInspector protocol) ──────────────────────────

    async def basic_get(self, queue: str) -> RabbitMessage | None:
        """Get a single message without subscribing.

        Used by DLQInspector for peek/replay. Returns None if the queue is empty.
        """
        await self._ensure_connected()
        assert self._topology_channel is not None
        q = await self._topology_channel.get_queue(queue, ensure=False)
        aio_msg = await q.get(fail=False, no_ack=False)
        if aio_msg is None:
            return None
        return self._build_message(aio_msg)

    async def purge_queue(self, queue: str) -> int:
        """Purge all messages from a queue. Returns the number of messages purged."""
        await self._ensure_connected()
        assert self._topology_channel is not None
        q = await self._topology_channel.get_queue(queue, ensure=False)
        result = await q.purge()
        return int(getattr(result, "message_count", 0))

    # ── Internal ──────────────────────────────────────────────────────────

    def _build_message(self, aio_message: Any) -> RabbitMessage:
        """Build RabbitMessage from aio-pika IncomingMessage."""
        message = RabbitMessage(
            body=aio_message.body,
            headers=dict(aio_message.headers) if aio_message.headers else {},
            message_id=aio_message.message_id,
            correlation_id=aio_message.correlation_id,
            reply_to=aio_message.reply_to,
            content_type=aio_message.content_type,
            content_encoding=aio_message.content_encoding,
            type=aio_message.type,
            app_id=aio_message.app_id,
            routing_key=aio_message.routing_key,
            exchange=aio_message.exchange or "",
            delivery_tag=aio_message.delivery_tag,
            redelivered=aio_message.redelivered,
            consumer_tag=aio_message.consumer_tag,
            raw_message=aio_message,
        )

        async def ack_fn() -> None:
            await aio_message.ack()

        async def nack_fn(requeue: bool = True) -> None:
            await aio_message.nack(requeue=requeue)

        async def reject_fn(requeue: bool = False) -> None:
            await aio_message.reject(requeue=requeue)

        message._ack_async_fn = ack_fn
        message._nack_async_fn = nack_fn
        message._reject_async_fn = reject_fn

        return message
