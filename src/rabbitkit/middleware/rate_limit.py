"""Rate-limiting middleware for message consumption.

Limits the rate of message processing using a **token bucket** algorithm.
Thread-safe for sync consumers; uses ``asyncio.sleep`` for async consumers.

Three behaviours when the rate is exceeded (``on_limited``):

* ``"wait"``  — sleep until a token is available (default, back-pressures the consumer)
* ``"nack"``  — nack with ``requeue=True`` so another consumer / retry can handle it
* ``"drop"``  — nack with ``requeue=False`` (message discarded / sent to DLQ)

Quick start
-----------
    from rabbitkit.middleware.rate_limit import RateLimitMiddleware, RateLimitConfig

    rate_mw = RateLimitMiddleware(
        RateLimitConfig(max_rate=100.0, burst=10, on_limited="wait")
    )

    @broker.subscriber(queue="events", middlewares=[rate_mw])
    async def handle_event(body: bytes) -> None:
        ...

Per-consumer vs broker-wide
----------------------------
Attach as a per-route middleware to scope the limit to one queue:

    @broker.subscriber(queue="high-volume", middlewares=[rate_mw])
    def handle(body: bytes) -> None: ...

Or attach broker-wide by passing it in the broker constructor's middleware
list (if supported by your broker version).

Combining with FlowController
------------------------------
``RateLimitMiddleware`` limits the *consumer* side (processing rate).
``FlowController`` / ``BackpressureConfig`` limits the *publisher* side.
Use both together for full end-to-end flow control:

    from rabbitkit import FlowController, BackpressureConfig
    from rabbitkit.middleware.rate_limit import RateLimitMiddleware, RateLimitConfig

    # Publisher-side: max 5 000 msgs/s
    fc = FlowController(BackpressureConfig(rate_limit=5000))

    # Consumer-side: process max 200 msgs/s, nack the rest
    rate_mw = RateLimitMiddleware(RateLimitConfig(max_rate=200, on_limited="nack"))
"""

from __future__ import annotations

import asyncio
import threading
import time
from dataclasses import dataclass
from typing import Any

from rabbitkit.core.message import RabbitMessage
from rabbitkit.middleware.base import BaseMiddleware


@dataclass(frozen=True, slots=True)
class RateLimitConfig:
    """Configuration for rate limiting.

    Attributes:
        max_rate: Maximum messages per second.
        burst: Maximum burst size above steady rate.
        on_limited: Behavior when rate exceeded: "wait", "nack", or "drop".
    """

    max_rate: float
    burst: int = 1
    on_limited: str = "wait"  # "wait" | "nack" | "drop"

    def __post_init__(self) -> None:
        if self.max_rate <= 0:
            raise ValueError("max_rate must be positive")
        if self.burst < 1:
            raise ValueError("burst must be >= 1")
        if self.on_limited not in ("wait", "nack", "drop"):
            raise ValueError(f"on_limited must be 'wait', 'nack', or 'drop', got '{self.on_limited}'")


class _TokenBucket:
    """Thread-safe token bucket for rate limiting."""

    __slots__ = ("_capacity", "_last_refill", "_lock", "_rate", "_tokens")

    def __init__(self, rate: float, capacity: int) -> None:
        self._rate = rate
        self._capacity = capacity
        self._tokens = float(capacity)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
        self._last_refill = now

    def try_acquire(self) -> bool:
        """Try to acquire a token without blocking. Returns True if acquired."""
        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    def wait_time(self) -> float:
        """Return seconds to wait until a token is available."""
        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                return 0.0
            deficit = 1.0 - self._tokens
            return deficit / self._rate


class RateLimitMiddleware(BaseMiddleware):
    """Limits message processing rate using a token bucket.

    Behavior when rate is exceeded (configurable via on_limited):
    - "wait": Sleep until a token is available (default)
    - "nack": Reject message with requeue=True (another consumer can try)
    - "drop": Reject message with requeue=False (message is lost/goes to DLQ)
    """

    def __init__(self, config: RateLimitConfig) -> None:
        self._config = config
        self._bucket = _TokenBucket(config.max_rate, config.burst)
        # Maximum time (s) a "wait" policy will block for a token before falling
        # back to drop semantics. Override per-instance if needed.
        self._wait_deadline: float = 30.0

    def consume_scope(
        self,
        call_next: Any,
        message: RabbitMessage,
    ) -> Any:
        """Rate-limit sync message processing.

        For ``on_limited="wait"`` the loop polls the token bucket until a token
        is acquired **or** ``wait_deadline`` (seconds, monotonic) expires. If the
        deadline elapses with no token, the message falls back to the configured
        drop/nack semantics so the handler is **never** invoked without a token.
        """
        if self._bucket.try_acquire():
            return call_next(message)

        if self._config.on_limited == "nack":
            if not message.is_settled:
                message.nack(requeue=True)
            return None
        if self._config.on_limited == "drop":
            if not message.is_settled:
                message.nack(requeue=False)
            return None

        # "wait" — bounded loop; only proceed once a token is actually acquired.
        deadline = time.monotonic() + self._wait_deadline
        while not self._bucket.try_acquire():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # No token within the deadline — fall back to drop semantics so
                # the handler is NOT called without a token.
                if not message.is_settled:
                    message.nack(requeue=False)
                return None
            time.sleep(min(self._bucket.wait_time(), remaining))

        return call_next(message)

    async def consume_scope_async(
        self,
        call_next: Any,
        message: RabbitMessage,
    ) -> Any:
        """Rate-limit async message processing.

        Mirrors the sync logic: the "wait" loop is bounded by
        ``self._wait_deadline`` and falls back to drop semantics if no token is
        acquired in time, so the handler is never called without a token.
        """
        if self._bucket.try_acquire():
            return await call_next(message)

        if self._config.on_limited == "nack":
            if not message.is_settled:
                await message.nack_async(requeue=True)
            return None
        if self._config.on_limited == "drop":
            if not message.is_settled:
                await message.nack_async(requeue=False)
            return None

        # "wait" — bounded loop; only proceed once a token is actually acquired.
        deadline = time.monotonic() + self._wait_deadline
        while not self._bucket.try_acquire():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                if not message.is_settled:
                    await message.nack_async(requeue=False)
                return None
            await asyncio.sleep(min(self._bucket.wait_time(), remaining))

        return await call_next(message)
