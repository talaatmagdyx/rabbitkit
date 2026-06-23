"""RPCClient — request/response over RabbitMQ.

Uses direct reply-to (amq.rabbitmq.reply-to) for lowest latency.
One client instance reuses one reply queue.
correlation_id: UUID per request, matched on response.
Timeout: configurable per call, raises RPCTimeoutError.

Error behavior (deliberate design choice):
- If handler raises and ExceptionMiddleware returns fallback with publish=True →
  fallback publishes to result_publisher (if @publisher set), NOT to reply_to.
- By default, RPC caller receives RPCTimeoutError on handler failure.
- This is intentional: exception context is not an RPC response.

Fast-failure pattern (RECOMMENDED for production RPC handlers):
- Handlers should catch exceptions and return standardized error envelopes
  explicitly, so callers fail fast instead of waiting for timeout.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import uuid
from typing import Any

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope

logger = logging.getLogger(__name__)


class RPCTimeoutError(TimeoutError):
    """Raised when an RPC call times out waiting for a response."""

    def __init__(self, correlation_id: str, timeout: float) -> None:
        self.correlation_id = correlation_id
        self.timeout = timeout
        super().__init__(
            f"RPC call timed out after {timeout}s (correlation_id={correlation_id})"
        )


class RPCClient:
    """Synchronous RPC client over RabbitMQ.

    Usage::

        client = RPCClient(transport)
        response = client.call("rpc.orders", b'{"id": 1}', timeout=5.0)
        print(response.body)
        client.close()
    """

    def __init__(
        self,
        transport: Any,
        *,
        serializer: Any | None = None,
        max_pending: int = 100,
    ) -> None:
        self._transport = transport
        self._serializer = serializer
        self._max_pending = max_pending
        self._reply_queue = "amq.rabbitmq.reply-to"

        # Pending calls: correlation_id → threading.Event + result container
        self._pending: dict[str, _PendingCall] = {}
        self._lock = threading.Lock()
        self._consuming = False
        self._consumer_tag: str | None = None

    def call(
        self,
        routing_key: str,
        body: bytes,
        *,
        timeout: float = 5.0,
        exchange: str = "",
        headers: dict[str, Any] | None = None,
    ) -> RabbitMessage:
        """Send an RPC request and wait for a response.

        Args:
            routing_key: The routing key (queue name) to send the request to.
            body: The request body.
            timeout: Maximum time to wait for a response (seconds).
            exchange: The exchange to publish to (default: "" for direct).
            headers: Optional headers to include in the request.

        Returns:
            RabbitMessage: The response message.

        Raises:
            RPCTimeoutError: If the response is not received within the timeout.
            RuntimeError: If max_pending calls is exceeded.
        """
        self._ensure_consuming()

        correlation_id = str(uuid.uuid4())

        with self._lock:
            if len(self._pending) >= self._max_pending:
                raise RuntimeError(
                    f"Max pending RPC calls ({self._max_pending}) exceeded. "
                    "Consider increasing max_pending or reducing call rate."
                )
            pending = _PendingCall()
            self._pending[correlation_id] = pending

        # Publish request
        envelope = MessageEnvelope(
            routing_key=routing_key,
            body=body,
            exchange=exchange,
            reply_to=self._reply_queue,
            correlation_id=correlation_id,
            headers=headers or {},
        )
        self._transport.publish(envelope)

        # Wait for response
        if not pending.event.wait(timeout=timeout):
            with self._lock:
                self._pending.pop(correlation_id, None)
            raise RPCTimeoutError(correlation_id, timeout)

        with self._lock:
            self._pending.pop(correlation_id, None)

        return pending.result  # type: ignore[return-value]

    def close(self) -> None:
        """Close the RPC client and clean up.

        Cancels the reply consumer and resolves all pending calls.
        """
        if self._consumer_tag and self._transport:
            try:
                self._transport.cancel_consumer(self._consumer_tag)
            except Exception as e:
                logger.warning("Failed to cancel RPC reply consumer: %s", e)

        # Clean up pending calls
        with self._lock:
            for _, pending in list(self._pending.items()):
                pending.event.set()  # unblock waiters
            self._pending.clear()

        self._consuming = False
        self._consumer_tag = None

    def _ensure_consuming(self) -> None:
        """Ensure the reply consumer is running.

        Holds the lock across the check-and-set so concurrent first callers
        from different threads don't each register a reply consumer.
        """
        with self._lock:
            if self._consuming:
                return

            def on_reply(message: RabbitMessage) -> None:
                """Handle reply messages — match by correlation_id."""
                cid = message.correlation_id
                if not cid:
                    logger.warning("RPC reply without correlation_id, discarding")
                    return

                with self._lock:
                    pending = self._pending.get(cid)
                    if pending is None:
                        logger.warning(
                            "Late or unknown RPC reply (correlation_id=%s), discarding",
                            cid,
                        )
                        return
                    pending.result = message
                    pending.event.set()

            self._consumer_tag = self._transport.consume(
                queue=self._reply_queue,
                callback=on_reply,
            )
            self._consuming = True


class AsyncRPCClient:
    """Asynchronous RPC client over RabbitMQ.

    Usage::

        client = AsyncRPCClient(transport)
        response = await client.call("rpc.orders", b'{"id": 1}', timeout=5.0)
        print(response.body)
        await client.close()
    """

    def __init__(
        self,
        transport: Any,
        *,
        serializer: Any | None = None,
        max_pending: int = 100,
    ) -> None:
        self._transport = transport
        self._serializer = serializer
        self._max_pending = max_pending
        self._reply_queue = "amq.rabbitmq.reply-to"

        # Pending calls: correlation_id → asyncio.Future
        self._pending: dict[str, asyncio.Future[RabbitMessage]] = {}
        self._lock = asyncio.Lock()
        self._consuming = False
        self._consumer_tag: str | None = None

    async def call(
        self,
        routing_key: str,
        body: bytes,
        *,
        timeout: float = 5.0,
        exchange: str = "",
        headers: dict[str, Any] | None = None,
    ) -> RabbitMessage:
        """Send an RPC request and wait for a response.

        Args:
            routing_key: The routing key (queue name) to send the request to.
            body: The request body.
            timeout: Maximum time to wait for a response (seconds).
            exchange: The exchange to publish to (default: "" for direct).
            headers: Optional headers to include in the request.

        Returns:
            RabbitMessage: The response message.

        Raises:
            RPCTimeoutError: If the response is not received within the timeout.
            RuntimeError: If max_pending calls is exceeded.
        """
        await self._ensure_consuming()

        correlation_id = str(uuid.uuid4())

        async with self._lock:
            if len(self._pending) >= self._max_pending:
                raise RuntimeError(
                    f"Max pending RPC calls ({self._max_pending}) exceeded. "
                    "Consider increasing max_pending or reducing call rate."
                )
            loop = asyncio.get_running_loop()
            future: asyncio.Future[RabbitMessage] = loop.create_future()
            self._pending[correlation_id] = future

        # Publish request
        envelope = MessageEnvelope(
            routing_key=routing_key,
            body=body,
            exchange=exchange,
            reply_to=self._reply_queue,
            correlation_id=correlation_id,
            headers=headers or {},
        )
        await self._transport.publish(envelope)

        # Wait for response with timeout
        try:
            result = await asyncio.wait_for(future, timeout=timeout)
        except TimeoutError:
            async with self._lock:
                self._pending.pop(correlation_id, None)
            raise RPCTimeoutError(correlation_id, timeout) from None

        async with self._lock:
            self._pending.pop(correlation_id, None)

        return result

    async def close(self) -> None:
        """Close the RPC client and clean up.

        Cancels the reply consumer and resolves all pending calls.
        """
        if self._consumer_tag and self._transport:
            try:
                await self._transport.cancel_consumer(self._consumer_tag)
            except Exception as e:
                logger.warning("Failed to cancel RPC reply consumer: %s", e)

        # Clean up pending calls
        async with self._lock:
            for _, future in list(self._pending.items()):
                if not future.done():
                    future.cancel()
            self._pending.clear()

        self._consuming = False
        self._consumer_tag = None

    async def _ensure_consuming(self) -> None:
        """Ensure the reply consumer is running.

        Uses the existing lock to prevent concurrent first-calls from each
        registering a duplicate consumer on amq.rabbitmq.reply-to (which
        supports exactly one consumer per channel).
        """
        async with self._lock:
            if self._consuming:
                return

            async def on_reply(message: RabbitMessage) -> None:
                """Handle reply messages — match by correlation_id."""
                cid = message.correlation_id
                if not cid:
                    logger.warning("RPC reply without correlation_id, discarding")
                    return

                async with self._lock:
                    future = self._pending.get(cid)
                    if future is None:
                        logger.warning(
                            "Late or unknown RPC reply (correlation_id=%s), discarding",
                            cid,
                        )
                        return
                    if not future.done():
                        future.set_result(message)

            self._consumer_tag = await self._transport.consume(
                queue=self._reply_queue,
                callback=on_reply,
            )
            self._consuming = True


class _PendingCall:
    """Internal container for a pending sync RPC call."""

    __slots__ = ("event", "result")

    def __init__(self) -> None:
        self.event = threading.Event()
        self.result: RabbitMessage | None = None
