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
import time
import uuid
from concurrent.futures import Future
from typing import Any

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import DIRECT_REPLY_TO_QUEUE, MessageEnvelope

logger = logging.getLogger(__name__)


class RPCTimeoutError(TimeoutError):
    """Raised when an RPC call times out waiting for a response."""

    def __init__(self, correlation_id: str, timeout: float) -> None:
        self.correlation_id = correlation_id
        self.timeout = timeout
        super().__init__(f"RPC call timed out after {timeout}s (correlation_id={correlation_id})")


class RPCClientClosed(RuntimeError):
    """Raised by :meth:`RPCClient.call` when the client has been closed.

    ``close()`` resolves all outstanding waiters with this error instead of
    leaving ``result=None`` (which previously caused ``AttributeError``).
    """


class ReplyTooLargeError(Exception):
    """Raised when an RPC reply body exceeds ``max_reply_bytes`` (L-8)."""

    def __init__(self, correlation_id: str, size: int, limit: int) -> None:
        self.correlation_id = correlation_id
        self.size = size
        self.limit = limit
        super().__init__(
            f"RPC reply (correlation_id={correlation_id}) body size {size} exceeds max_reply_bytes limit {limit}"
        )


class _ReplyConnection:
    """Minimal pika ``BlockingConnection``-like interface used by RPCClient.

    ``RPCClient`` owns a *dedicated* reply connection so it can pump the I/O
    loop itself while waiting for a reply. This avoids deadlocking the broker's
    transport I/O thread when sync RPC is called from a handler (especially
    under ``worker_count=1``).
    """

    def process_data_events(self, time_limit: float = 0.0) -> None:
        raise NotImplementedError


# ── Shared reply-routing infrastructure ───────────────────────────────────


class _Sink:
    """Abstract sink wrapping a Future for RPC reply resolution.

    Both ``concurrent.futures.Future`` (sync) and ``asyncio.Future`` (async)
    satisfy the ``set_result`` / ``set_exception`` / ``done`` / ``cancel``
    contract, but the two Future types are not assignment-compatible under
    ``mypy --strict``. This base unifies them so :class:`_ReplyRouter` can hold
    a single ``dict[str, _Sink]``.
    """

    __slots__ = ()

    def set_result(self, msg: RabbitMessage) -> None:
        raise NotImplementedError

    def set_exception(self, exc: BaseException) -> None:
        raise NotImplementedError

    def done(self) -> bool:
        raise NotImplementedError

    def cancel(self) -> bool:
        raise NotImplementedError


class _FutureSink(_Sink):
    """Sink wrapping ``concurrent.futures.Future`` for the sync RPC client."""

    __slots__ = ("_fut",)

    def __init__(self, fut: Future[RabbitMessage]) -> None:
        self._fut = fut

    def set_result(self, msg: RabbitMessage) -> None:
        self._fut.set_result(msg)

    def set_exception(self, exc: BaseException) -> None:
        self._fut.set_exception(exc)

    def done(self) -> bool:
        return self._fut.done()

    def cancel(self) -> bool:
        return self._fut.cancel()


class _AsyncFutureSink(_Sink):
    """Sink wrapping ``asyncio.Future`` for the async RPC client."""

    __slots__ = ("_fut",)

    def __init__(self, fut: asyncio.Future[RabbitMessage]) -> None:
        self._fut = fut

    def set_result(self, msg: RabbitMessage) -> None:
        self._fut.set_result(msg)

    def set_exception(self, exc: BaseException) -> None:
        self._fut.set_exception(exc)

    def done(self) -> bool:
        return self._fut.done()

    def cancel(self) -> bool:
        return self._fut.cancel()


class _ReplyRouter:
    """Shared reply router for sync and async RPC clients.

    Holds the pending-call registry (``correlation_id → _Sink``), the
    reply-body size cap (``max_reply_bytes``) and the pending cap
    (``max_pending``). The :meth:`resolve` method implements the reply
    callback body — correlation matching, size check, and result/exception
    resolution — in one place; the sync and async clients only differ in
    *how* they lock around it (``threading.Lock`` vs ``asyncio.Lock``).

    The caller of :meth:`register`, :meth:`pop`, :meth:`resolve`,
    :meth:`close_all`, and :meth:`cancel_all` must hold whatever lock guards
    ``_pending``; this class does no locking of its own.
    """

    __slots__ = ("_pending", "max_pending", "max_reply_bytes")

    def __init__(self, *, max_reply_bytes: int | None, max_pending: int) -> None:
        self.max_reply_bytes = max_reply_bytes
        self.max_pending = max_pending
        self._pending: dict[str, _Sink] = {}

    def is_full(self) -> bool:
        return len(self._pending) >= self.max_pending

    def register(self, cid: str, sink: _Sink) -> None:
        self._pending[cid] = sink

    def pop(self, cid: str) -> _Sink | None:
        return self._pending.pop(cid, None)

    def pending_count(self) -> int:
        return len(self._pending)

    def resolve(self, message: RabbitMessage) -> None:
        """Shared reply body: correlation match → size check → resolve sink.

        Must be called holding whatever lock guards ``_pending``.
        """
        cid = message.correlation_id
        if not cid:
            logger.warning("RPC reply without correlation_id, discarding")
            return

        sink = self._pending.get(cid)
        if sink is None:
            logger.warning(
                "Late or unknown RPC reply (correlation_id=%s), discarding",
                cid,
            )
            return

        if sink.done():
            return

        # L-8: cap reply body size so a runaway/zip-bomb reply can't
        # materialise a huge buffer in the caller; surface as a
        # ReplyTooLargeError instead of storing the result.
        if self.max_reply_bytes is not None and len(message.body) > self.max_reply_bytes:
            sink.set_exception(ReplyTooLargeError(cid, len(message.body), self.max_reply_bytes))
            return

        sink.set_result(message)

    def close_all(self, exc: BaseException) -> None:
        """Resolve every pending sink with *exc* and clear the registry.

        Used by the sync client's ``close()`` so waiters raise cleanly.
        Must be called holding whatever lock guards ``_pending``.
        """
        for sink in self._pending.values():
            if not sink.done():
                sink.set_exception(exc)
        self._pending.clear()

    def cancel_all(self) -> None:
        """Cancel every pending sink and clear the registry.

        Used by the async client's ``close()`` to cancel outstanding futures.
        Must be called holding whatever lock guards ``_pending``.
        """
        for sink in self._pending.values():
            if not sink.done():
                sink.cancel()
        self._pending.clear()


# ── Sync RPC client ───────────────────────────────────────────────────────


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
        reply_connection: Any | None = None,
        close_reply_connection: bool = False,
        max_reply_bytes: int | None = None,
    ) -> None:
        self._transport = transport
        self._serializer = serializer
        self._reply_queue = DIRECT_REPLY_TO_QUEUE

        # Shared reply router holds max_reply_bytes / max_pending / _pending.
        self._router = _ReplyRouter(
            max_reply_bytes=max_reply_bytes,
            max_pending=max_pending,
        )

        # Dedicated reply connection. When provided, ``call()`` pumps it via
        # ``process_data_events`` while waiting so the broker's I/O thread is
        # never blocked. When ``None`` (e.g. in tests using a transport mock),
        # ``call()`` falls back to blocking on the future — which works when
        # replies are delivered out-of-band (the existing test harness).
        #
        # Ownership: by default the caller owns *reply_connection* and is
        # responsible for closing it (it may be shared). Set
        # ``close_reply_connection=True`` to have close() close it too.
        self._connection: Any | None = reply_connection
        self._close_reply_connection = bool(close_reply_connection)

        self._lock = threading.Lock()
        self._consuming = False
        self._consumer_tag: str | None = None
        self._starting = False
        # L-5: guards call()/_ensure_consuming() after close().
        self._closed = False

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
            RPCClientClosed: If the client was closed while waiting.
            RuntimeError: If max_pending calls is exceeded.

        Note:
            Sync RPC requires its own dedicated reply connection (passed via
            ``reply_connection``) so the I/O loop can be pumped while waiting.
            Calling sync RPC from inside a ``worker_count=1`` sync handler with
            a shared broker connection would otherwise deadlock.
        """
        # L-5: refuse to operate after close() instead of silently re-registering
        # a consumer on a half-torn-down client.
        if self._closed:
            raise RPCClientClosed("RPCClient is closed")
        self._ensure_consuming()

        correlation_id = str(uuid.uuid4())

        with self._lock:
            if self._router.is_full():
                raise RuntimeError(
                    f"Max pending RPC calls ({self._router.max_pending}) exceeded. "
                    "Consider increasing max_pending or reducing call rate."
                )
            fut: Future[RabbitMessage] = Future()
            self._router.register(correlation_id, _FutureSink(fut))

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

        # Wait for the response. If we own a dedicated reply connection, pump
        # its I/O loop ourselves; otherwise block directly on the future
        # (replies are delivered out-of-band, e.g. by a test harness or another
        # thread).
        # L-4: wrap the pump in try/finally so a `process_data_events` that
        # raises still pops the _pending entry — otherwise it leaks and
        # exhausts `max_pending` over time.
        deadline = time.monotonic() + timeout
        try:
            if self._connection is not None:
                while not fut.done():
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        break
                    self._connection.process_data_events(time_limit=min(0.01, remaining))
            else:
                remaining = deadline - time.monotonic()
                if remaining > 0:
                    try:
                        fut.result(timeout=remaining)
                    except TimeoutError:
                        pass  # handled as timeout below
        finally:
            if not fut.done():
                with self._lock:
                    self._router.pop(correlation_id)

        if not fut.done():
            raise RPCTimeoutError(correlation_id, timeout)

        with self._lock:
            self._router.pop(correlation_id)

        # Raises the stored exception (ReplyTooLargeError / RPCClientClosed)
        # or returns the resolved result.
        return fut.result()

    def close(self) -> None:
        """Close the RPC client and clean up.

        Cancels the reply consumer and resolves all pending waiters with an
        ``RPCClientClosed`` error so callers fail cleanly instead of hitting
        ``AttributeError`` on a ``None`` result.

        When constructed with ``close_reply_connection=True`` AND a
        ``reply_connection``, also closes the dedicated reply connection
        (ownership transferred to the client). Otherwise the caller retains
        ownership of *reply_connection* and must close it themselves.
        """
        if self._consumer_tag and self._transport:
            try:
                self._transport.cancel_consumer(self._consumer_tag)
            except Exception as e:
                logger.warning("Failed to cancel RPC reply consumer: %s", e)

        # Clean up pending calls — set an exception so waiters raise cleanly.
        # L-5: mark the client closed so subsequent call()/_ensure_consuming()
        # refuse to re-register a consumer on the torn-down client.
        with self._lock:
            self._closed = True
            self._router.close_all(RPCClientClosed("RPCClient was closed"))

        self._consuming = False
        self._consumer_tag = None

        # Low: optionally close the dedicated reply connection when the client
        # owns it. Default keeps the prior behaviour (caller-owned).
        if self._close_reply_connection and self._connection is not None:
            try:
                self._connection.close()
            except Exception as e:
                logger.warning("Failed to close RPC reply connection: %s", e)

    def _ensure_consuming(self) -> None:
        """Ensure the reply consumer is running.

        The network ``consume()`` call is made *outside* the lock to avoid
        self-deadlock and holding the lock across I/O. A ``_starting`` flag
        guards against concurrent first-callers each registering a consumer.
        """
        # L-5: refuse to (re-)register a consumer on a closed client.
        if self._closed:
            raise RPCClientClosed("RPCClient is closed")
        with self._lock:
            if self._consuming or self._starting:
                return
            self._starting = True

        def on_reply(message: RabbitMessage) -> None:
            """Handle reply messages — delegate correlation match to the router."""
            with self._lock:
                self._router.resolve(message)

        try:
            # amq.rabbitmq.reply-to is a broker pseudo-queue: it rejects any
            # Queue.Declare (declare=False) and requires a no-ack consumer
            # (no_ack=True) — the broker auto-acks each reply on delivery.
            consumer_tag = self._transport.consume(
                queue=self._reply_queue,
                callback=on_reply,
                no_ack=True,
                declare=False,
            )
        except Exception:
            with self._lock:
                self._starting = False
            raise

        with self._lock:
            self._consumer_tag = consumer_tag
            self._consuming = True
            self._starting = False


# ── Async RPC client ──────────────────────────────────────────────────────


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
        max_reply_bytes: int | None = None,
    ) -> None:
        self._transport = transport
        self._serializer = serializer
        self._reply_queue = DIRECT_REPLY_TO_QUEUE

        # Shared reply router holds max_reply_bytes / max_pending / _pending.
        self._router = _ReplyRouter(
            max_reply_bytes=max_reply_bytes,
            max_pending=max_pending,
        )

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
            if self._router.is_full():
                raise RuntimeError(
                    f"Max pending RPC calls ({self._router.max_pending}) exceeded. "
                    "Consider increasing max_pending or reducing call rate."
                )
            loop = asyncio.get_running_loop()
            future: asyncio.Future[RabbitMessage] = loop.create_future()
            self._router.register(correlation_id, _AsyncFutureSink(future))

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

        # Wait for response with timeout. R-timeout: asyncio.timeout (3.11+)
        # replaces asyncio.wait_for to avoid the wrapper-task overhead.
        try:
            async with asyncio.timeout(timeout):
                result = await future
        except TimeoutError:
            async with self._lock:
                self._router.pop(correlation_id)
            raise RPCTimeoutError(correlation_id, timeout) from None

        async with self._lock:
            self._router.pop(correlation_id)

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
            self._router.cancel_all()

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
                """Handle reply messages — delegate correlation match to the router."""
                async with self._lock:
                    self._router.resolve(message)

            # amq.rabbitmq.reply-to is a broker pseudo-queue: it rejects any
            # Queue.Declare (declare=False) and requires a no-ack consumer
            # (no_ack=True) — the broker auto-acks each reply on delivery.
            self._consumer_tag = await self._transport.consume(
                queue=self._reply_queue,
                callback=on_reply,
                no_ack=True,
                declare=False,
            )
            self._consuming = True
