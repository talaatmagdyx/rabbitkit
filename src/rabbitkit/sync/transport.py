"""SyncTransport — pika-based transport adapter.

THREAD SAFETY (CRITICAL):
Model A — One connection per thread (used in 0.1.0):
Each thread gets its own dedicated pika connection.
No cross-thread connection sharing.

Fork safety: lazy connect (NOT in __init__) — pika sockets can't cross fork().
Reconnection: _ensure_connected() before each publish.
TopologyMode: respected in declare_exchange/declare_queue/bind_queue.
"""

from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable
from typing import Any, TypeVar

from rabbitkit.core.config import ConnectionConfig, SecurityConfig, SocketConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import (
    MessageEnvelope,
    PublishOutcome,
    PublishStatus,
    TopologyMode,
)
from rabbitkit.sync.connection import get_connection_errors, make_pika_connection_params

logger = logging.getLogger(__name__)

_T = TypeVar("_T")


class SyncTransport:
    """Pika-based synchronous transport.

    Lazy connect: connection is established on first use, not in __init__.
    This ensures fork safety and avoids connection issues during import.

    THE INVARIANT: no pika connection is ever used from a thread other than
    the one that created it.
    """

    def __init__(
        self,
        connection_config: ConnectionConfig | None = None,
        socket_config: SocketConfig | None = None,
        security_config: SecurityConfig | None = None,
        topology_mode: TopologyMode = TopologyMode.AUTO_DECLARE,
        confirm_delivery: bool = True,
    ) -> None:
        self._connection_config = connection_config or ConnectionConfig()
        self._socket_config = socket_config or SocketConfig()
        self._security_config = security_config or SecurityConfig()
        self._topology_mode = topology_mode
        self._confirm_delivery = confirm_delivery

        self._connection: Any = None  # pika.BlockingConnection
        self._channel: Any = None  # pika.channel.Channel
        self._connected = False
        self._consumer_tags: dict[str, str] = {}  # queue_name → consumer_tag
        self._owner_ident: int | None = None  # thread that owns the connection
        self._consuming = False  # True while the I/O loop is running

    def connect(self) -> None:
        """Establish connection to RabbitMQ."""
        if self._connected:
            return

        try:
            import pika
        except ImportError:
            raise ImportError(
                "pika is required for sync transport. "
                "Install it with: pip install rabbitkit[sync]"
            ) from None

        params = make_pika_connection_params(
            self._connection_config,
            self._socket_config,
            self._security_config,
        )

        logger.info(
            "Connecting to RabbitMQ at %s:%d",
            self._connection_config.host,
            self._connection_config.port,
        )

        self._connection = pika.BlockingConnection(params)
        self._channel = self._connection.channel()

        if self._confirm_delivery:
            self._channel.confirm_delivery()

        self._connected = True
        self._owner_ident = threading.get_ident()
        logger.info("Connected to RabbitMQ")

    def disconnect(self) -> None:
        """Close connection to RabbitMQ."""
        if not self._connected:
            return

        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception as e:
            logger.warning("Error during disconnect: %s", e)
        finally:
            self._connection = None
            self._channel = None
            self._connected = False
            self._owner_ident = None
            logger.info("Disconnected from RabbitMQ")

    def is_connected(self) -> bool:
        """Check if connected to RabbitMQ."""
        if not self._connected:
            return False
        try:
            return (
                self._connection is not None
                and self._connection.is_open
                and self._channel is not None
                and self._channel.is_open
            )
        except Exception:
            return False

    def _ensure_connected(self) -> None:
        """Ensure connection is established, reconnecting if needed."""
        if self.is_connected():
            return

        self._connected = False
        backoff = self._connection_config.reconnect_backoff_base
        max_backoff = self._connection_config.reconnect_backoff_max
        connection_errors = get_connection_errors()

        while True:
            try:
                self.connect()
                return
            except connection_errors as e:
                logger.warning(
                    "Connection failed, retrying in %.1fs: %s",
                    backoff,
                    e,
                )
                time.sleep(backoff)
                backoff = min(backoff * 2, max_backoff)

    def reconnect(self) -> None:
        """Force a fresh connection + channel (used by consumer recovery)."""
        self.disconnect()
        self._ensure_connected()

    def _run_on_io_thread(self, fn: Callable[[], _T]) -> _T:
        """Run a channel operation on the connection's I/O thread.

        pika's BlockingConnection is NOT thread-safe: every basic_* call must
        execute on the thread that owns the connection. When a worker thread
        (worker_count > 1) acks/nacks/publishes, marshal the call onto the I/O
        loop via add_callback_threadsafe and block for its result/exception.
        When already on the owner thread (single worker / publisher), or when
        no consume loop is running to drain callbacks, run inline.
        """
        if (
            not self._consuming
            or self._owner_ident is None
            or threading.get_ident() == self._owner_ident
        ):
            return fn()

        result: list[_T] = []
        error: list[BaseException] = []
        done = threading.Event()

        def _cb() -> None:
            try:
                result.append(fn())
            except BaseException as exc:  # re-raised on the caller thread
                error.append(exc)
            finally:
                done.set()

        # ponytail: blocks until the I/O loop drains the callback; a dead loop
        # is handled one level up by consumer recovery, not a timeout here.
        self._connection.add_callback_threadsafe(_cb)
        done.wait()
        if error:
            raise error[0]
        return result[0]

    def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        """Publish a message to RabbitMQ.

        Returns PublishOutcome with status indicating success/failure.
        """
        self._ensure_connected()

        try:
            import pika

            properties = pika.BasicProperties(
                message_id=envelope.message_id,
                correlation_id=envelope.correlation_id,
                reply_to=envelope.reply_to,
                content_type=envelope.content_type,
                content_encoding=envelope.content_encoding,
                headers=envelope.headers or None,
                delivery_mode=envelope.delivery_mode,
                priority=envelope.priority,
                expiration=envelope.expiration,
                type=envelope.type,
                user_id=envelope.user_id,
                app_id=envelope.app_id,
            )

            if envelope.timestamp:
                properties.timestamp = int(envelope.timestamp.timestamp())

            self._run_on_io_thread(
                lambda: self._channel.basic_publish(
                    exchange=envelope.exchange,
                    routing_key=envelope.routing_key,
                    body=envelope.body,
                    properties=properties,
                    mandatory=envelope.mandatory,
                )
            )

            return PublishOutcome(
                status=PublishStatus.CONFIRMED,
                exchange=envelope.exchange,
                routing_key=envelope.routing_key,
            )

        except Exception as e:
            logger.error("Publish failed: %s", e)
            return PublishOutcome(
                status=PublishStatus.ERROR,
                exchange=envelope.exchange,
                routing_key=envelope.routing_key,
                error=e,
            )

    def consume(
        self,
        queue: str,
        callback: Callable[[RabbitMessage], None],
        prefetch: int = 10,
    ) -> str:
        """Start consuming from a queue.

        Returns the consumer tag.
        """
        self._ensure_connected()

        self._channel.basic_qos(prefetch_count=prefetch)

        consumer_tag = f"rabbitkit.{uuid.uuid4()}"

        def on_message(ch: Any, method: Any, properties: Any, body: bytes) -> None:
            """Internal pika callback — builds RabbitMessage and calls user callback."""
            message = self._build_message(method, properties, body)
            callback(message)

        self._channel.basic_consume(
            queue=queue,
            on_message_callback=on_message,
            auto_ack=False,
            consumer_tag=consumer_tag,
        )

        self._consumer_tags[queue] = consumer_tag
        logger.info("Started consuming from queue '%s' with tag '%s'", queue, consumer_tag)
        return consumer_tag

    def declare_exchange(self, exchange: RabbitExchange) -> None:
        """Declare an exchange on RabbitMQ."""
        if self._topology_mode == TopologyMode.MANUAL:
            return

        self._ensure_connected()

        kwargs = exchange.to_declare_kwargs()

        if self._topology_mode == TopologyMode.PASSIVE_ONLY or exchange.passive:
            self._channel.exchange_declare(
                exchange=kwargs["exchange"],
                passive=True,
            )
        else:
            self._channel.exchange_declare(**kwargs)

    def declare_queue(self, queue: RabbitQueue) -> None:
        """Declare a queue on RabbitMQ."""
        if self._topology_mode == TopologyMode.MANUAL:
            return

        self._ensure_connected()

        kwargs = queue.to_declare_kwargs()

        if self._topology_mode == TopologyMode.PASSIVE_ONLY or queue.passive:
            self._channel.queue_declare(
                queue=kwargs["queue"],
                passive=True,
            )
        else:
            self._channel.queue_declare(**kwargs)

    def bind_queue(self, queue: str, exchange: str, routing_key: str) -> None:
        """Bind a queue to an exchange."""
        if self._topology_mode == TopologyMode.MANUAL:
            return

        self._ensure_connected()

        self._channel.queue_bind(
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
        )

    def bind_exchange(
        self,
        destination: str,
        source: str,
        routing_key: str = "",
        arguments: dict[str, Any] | None = None,
    ) -> None:
        """Bind an exchange to another exchange (exchange-to-exchange binding)."""
        if self._topology_mode == TopologyMode.MANUAL:
            return

        self._ensure_connected()

        self._channel.exchange_bind(
            destination=destination,
            source=source,
            routing_key=routing_key,
            arguments=arguments,
        )

    def cancel_consumer(self, consumer_tag: str) -> None:
        """Cancel a consumer by tag."""
        if not self.is_connected():
            return

        try:
            self._channel.basic_cancel(consumer_tag=consumer_tag)
        except Exception as e:
            logger.warning("Failed to cancel consumer %s: %s", consumer_tag, e)

        # Remove from tracking
        self._consumer_tags = {
            q: t for q, t in self._consumer_tags.items() if t != consumer_tag
        }

    def start_consuming(self) -> None:
        """Start the pika consume loop (blocking)."""
        self._ensure_connected()
        self._consuming = True
        try:
            self._channel.start_consuming()
        except KeyboardInterrupt:
            self._channel.stop_consuming()
        finally:
            self._consuming = False

    def stop_consuming(self) -> None:
        """Stop the pika consume loop."""
        if self._channel and self._channel.is_open:
            self._channel.stop_consuming()

    # ── DLQ / inspection (DLQInspector protocol) ──────────────────────────

    def basic_get(self, queue: str) -> RabbitMessage | None:
        """Get a single message without subscribing (auto_ack=False).

        Used by DLQInspector for peek/replay. Returns None if the queue is empty.
        """
        self._ensure_connected()
        method, properties, body = self._run_on_io_thread(
            lambda: self._channel.basic_get(queue=queue, auto_ack=False)
        )
        if method is None:
            return None
        return self._build_message(method, properties, body)

    def purge_queue(self, queue: str) -> int:
        """Purge all messages from a queue. Returns the number of messages purged."""
        self._ensure_connected()
        frame = self._run_on_io_thread(lambda: self._channel.queue_purge(queue=queue))
        return int(frame.method.message_count)

    # ── Internal ──────────────────────────────────────────────────────────

    def _build_message(self, method: Any, properties: Any, body: bytes) -> RabbitMessage:
        """Build RabbitMessage from pika delivery."""
        message = RabbitMessage(
            body=body,
            headers=dict(properties.headers) if properties.headers else {},
            message_id=properties.message_id,
            correlation_id=properties.correlation_id,
            reply_to=properties.reply_to,
            content_type=properties.content_type,
            content_encoding=properties.content_encoding,
            type=properties.type,
            app_id=properties.app_id,
            routing_key=method.routing_key,
            exchange=method.exchange,
            delivery_tag=method.delivery_tag,
            redelivered=method.redelivered,
            consumer_tag=getattr(method, "consumer_tag", None),  # absent on basic_get (Basic.GetOk)
        )

        # Wire sync settlement functions
        channel = self._channel

        def ack_fn() -> None:
            self._run_on_io_thread(
                lambda: channel.basic_ack(delivery_tag=method.delivery_tag)
            )

        def nack_fn(requeue: bool = True) -> None:
            self._run_on_io_thread(
                lambda: channel.basic_nack(
                    delivery_tag=method.delivery_tag, requeue=requeue
                )
            )

        def reject_fn(requeue: bool = False) -> None:
            self._run_on_io_thread(
                lambda: channel.basic_reject(
                    delivery_tag=method.delivery_tag, requeue=requeue
                )
            )

        message._ack_fn = ack_fn
        message._nack_fn = nack_fn
        message._reject_fn = reject_fn

        return message
