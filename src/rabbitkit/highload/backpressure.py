"""Publish-side flow control — backpressure, rate limiting, in-flight tracking.

``FlowController`` is NOT a middleware — it is used by transports/brokers
to throttle outgoing publishes when the system is under pressure.

Three pressure signals:
1. **connection.blocked** — RabbitMQ signals memory/disk alarm
2. **in-flight limit** — max concurrent unconfirmed publishes
3. **rate limit** — token-bucket limiter (messages per second)

Configurable behaviour when blocked:
- ``"wait"``  — block until unblocked / slot available / token available
- ``"raise"`` — raise ``BackpressureError`` immediately
- ``"drop"``  — return ``False`` (caller should discard the message)
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time

from rabbitkit.core.config import BackpressureConfig
from rabbitkit.core.errors import BackpressureError

logger = logging.getLogger(__name__)


# ── Token bucket rate limiter ────────────────────────────────────────────


class _TokenBucket:
    """Simple token-bucket rate limiter (sync, threading.Lock).

    Refills ``rate`` tokens per second.  ``acquire()`` consumes one token.
    Thread-safe via ``threading.Lock``.  Use in sync contexts only.
    """

    def __init__(self, rate: int, poll_interval: float = 0.01) -> None:
        self._rate = rate
        self._poll_interval = poll_interval
        self._tokens = float(rate)
        self._max_tokens = float(rate)
        self._last_refill = time.monotonic()
        self._lock = threading.Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._max_tokens, self._tokens + elapsed * self._rate)
        self._last_refill = now

    def acquire(self) -> bool:
        """Try to consume one token.  Returns True if available."""
        with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    def wait(self, timeout: float | None = None) -> bool:
        """Block until a token is available or timeout expires.

        Returns True if token acquired, False on timeout.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self.acquire():
                return True
            if deadline is not None and time.monotonic() >= deadline:
                return False
            # Sleep a short interval before retry
            time.sleep(self._poll_interval)


class _AsyncTokenBucket:
    """Async-native token-bucket rate limiter.

    Same algorithm as ``_TokenBucket`` but uses ``asyncio.Lock`` so it never
    blocks the event loop.  Instantiated lazily inside the event loop.
    """

    def __init__(self, rate: int) -> None:
        self._rate = rate
        self._tokens = float(rate)
        self._max_tokens = float(rate)
        self._last_refill = time.monotonic()
        self._lock: asyncio.Lock | None = None  # created lazily inside loop

    def _ensure_lock(self) -> asyncio.Lock:
        if self._lock is None:
            self._lock = asyncio.Lock()
        return self._lock

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self._max_tokens, self._tokens + elapsed * self._rate)
        self._last_refill = now

    async def acquire(self) -> bool:
        """Try to consume one token (non-blocking).  Returns True if available."""
        async with self._ensure_lock():
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False

    async def wait(self, timeout: float | None = None) -> bool:
        """Async-wait until a token is available or timeout expires.

        Calculates the exact time until the next token refills instead of
        polling every 10 ms, reducing unnecessary wakeups at high rates.
        """
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            async with self._ensure_lock():
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return True
                # Calculate how long until one token is available
                tokens_needed = 1.0 - self._tokens
                sleep_for = tokens_needed / self._rate
                if deadline is not None:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    sleep_for = min(sleep_for, remaining)
            await asyncio.sleep(sleep_for)


# ── FlowController ──────────────────────────────────────────────────────


class FlowController:
    """Publish-side flow control.

    Usage::

        fc = FlowController(BackpressureConfig(max_in_flight=500, rate_limit=1000))

        # Before publish:
        if fc.acquire(timeout=5.0):
            transport.publish(envelope)
            fc.release()
        else:
            # message dropped or error raised (depends on on_blocked)
            ...

        # Register with transport callbacks:
        transport.on_blocked(fc.on_blocked)
        transport.on_unblocked(fc.on_unblocked)
    """

    def __init__(self, config: BackpressureConfig | None = None) -> None:
        self._config = config or BackpressureConfig()
        self._blocked = False
        self._in_flight = 0
        self._lock = threading.Lock()
        self._unblock_event = threading.Event()
        self._unblock_event.set()  # start unblocked
        self._slot_event = threading.Event()
        self._slot_event.set()  # start with slots available

        # Async equivalents
        self._async_lock: asyncio.Lock | None = None  # lazily created
        self._async_unblock_event: asyncio.Event | None = None  # lazily created
        self._async_slot_event: asyncio.Event | None = None  # lazily created

        # Rate limiter (optional)
        # Sync path uses threading.Lock-based bucket; async path uses asyncio.Lock
        # so acquiring tokens never blocks the event loop.
        self._rate_limiter: _TokenBucket | None = None
        self._async_rate_limiter: _AsyncTokenBucket | None = None
        if self._config.rate_limit is not None:
            poll = self._config.poll_interval_ms / 1000.0
            self._rate_limiter = _TokenBucket(self._config.rate_limit, poll_interval=poll)
            self._async_rate_limiter = _AsyncTokenBucket(self._config.rate_limit)

    def _ensure_async_primitives(self) -> None:
        """Lazily create asyncio primitives (must be called in event loop)."""
        if self._async_lock is None:
            self._async_lock = asyncio.Lock()
        if self._async_unblock_event is None:
            self._async_unblock_event = asyncio.Event()
            if not self._blocked:
                self._async_unblock_event.set()
        if self._async_slot_event is None:
            self._async_slot_event = asyncio.Event()
            self._async_slot_event.set()

    # ── Connection blocked/unblocked callbacks ───────────────────────────

    def on_blocked(self) -> None:
        """Called when RabbitMQ signals connection.blocked."""
        self._blocked = True
        self._unblock_event.clear()
        if self._async_unblock_event is not None:
            self._async_unblock_event.clear()
        logger.warning("Connection blocked — backpressure active")

    def on_unblocked(self) -> None:
        """Called when RabbitMQ signals connection.unblocked."""
        self._blocked = False
        self._unblock_event.set()
        if self._async_unblock_event is not None:
            self._async_unblock_event.set()
        logger.info("Connection unblocked — backpressure released")

    # ── Properties ───────────────────────────────────────────────────────

    @property
    def is_blocked(self) -> bool:
        """True if connection is currently blocked by RabbitMQ."""
        return self._blocked

    @property
    def in_flight(self) -> int:
        """Current number of in-flight (unconfirmed) publishes."""
        return self._in_flight

    # ── Sync acquire / release ───────────────────────────────────────────

    def acquire(self, timeout: float | None = None) -> bool:
        """Acquire a publish slot.

        Checks (in order): not blocked, in-flight < max, rate OK.

        Returns True if slot acquired, False if dropped.
        Raises BackpressureError if ``on_blocked == "raise"`` and blocked.
        """
        effective_timeout = timeout if timeout is not None else self._config.blocked_timeout

        # 1. Check blocked state
        if self._blocked:
            if self._config.on_blocked == "raise":
                raise BackpressureError("Connection is blocked by RabbitMQ")
            if self._config.on_blocked == "drop":
                return False
            # "wait" — block until unblocked
            if not self._unblock_event.wait(timeout=effective_timeout):
                if self._config.on_blocked == "raise":  # pragma: no cover
                    raise BackpressureError("Timeout waiting for connection unblock")  # pragma: no cover
                return False

        # 2. Check in-flight limit
        with self._lock:
            if self._in_flight >= self._config.max_in_flight:
                if self._config.on_blocked == "raise":
                    raise BackpressureError(
                        f"In-flight limit reached ({self._config.max_in_flight})"
                    )
                if self._config.on_blocked == "drop":
                    return False
                # "wait" — need to wait for a slot
                pass
            else:
                # Slot available — check rate limiter
                if self._rate_limiter is not None:
                    if not self._rate_limiter.acquire():
                        if self._config.on_blocked == "raise":
                            raise BackpressureError("Rate limit exceeded")
                        if self._config.on_blocked == "drop":
                            return False
                        # "wait"
                        if not self._rate_limiter.wait(timeout=effective_timeout):
                            return False

                self._in_flight += 1
                if self._in_flight >= self._config.max_in_flight:
                    self._slot_event.clear()
                return True

        # Wait for a slot (on_blocked == "wait" and at in-flight limit)
        if not self._slot_event.wait(timeout=effective_timeout):
            return False

        # Retry after slot opened
        with self._lock:
            if self._in_flight >= self._config.max_in_flight:
                return False
            if self._rate_limiter is not None and not self._rate_limiter.acquire():
                return False
            self._in_flight += 1
            if self._in_flight >= self._config.max_in_flight:
                self._slot_event.clear()
            return True

    def release(self) -> None:
        """Release a publish slot after confirm/nack/timeout."""
        with self._lock:
            if self._in_flight > 0:
                self._in_flight -= 1
            self._slot_event.set()

    # ── Async acquire / release ──────────────────────────────────────────

    async def acquire_async(self, timeout: float | None = None) -> bool:
        """Async variant of ``acquire``."""
        self._ensure_async_primitives()
        assert self._async_lock is not None
        assert self._async_unblock_event is not None
        effective_timeout = timeout if timeout is not None else self._config.blocked_timeout

        # 1. Check blocked state
        if self._blocked:
            if self._config.on_blocked == "raise":
                raise BackpressureError("Connection is blocked by RabbitMQ")
            if self._config.on_blocked == "drop":
                return False
            # "wait"
            try:
                await asyncio.wait_for(
                    self._async_unblock_event.wait(),
                    timeout=effective_timeout,
                )
            except TimeoutError:
                return False

        # 2. Check in-flight limit
        at_limit = False
        async with self._async_lock:
            if self._in_flight < self._config.max_in_flight:
                # Slot available — check rate limiter and acquire immediately
                if self._async_rate_limiter is not None and not await self._async_rate_limiter.acquire():
                    if self._config.on_blocked == "raise":
                        raise BackpressureError("Rate limit exceeded")
                    return False  # "drop" or "wait" with no tokens: drop
                self._in_flight += 1
                return True
            # At limit — handle per on_blocked policy
            if self._config.on_blocked == "raise":
                raise BackpressureError(
                    f"In-flight limit reached ({self._config.max_in_flight})"
                )
            if self._config.on_blocked == "drop":
                return False
            # "wait" — release lock, wait for slot event, then retry
            at_limit = True
            assert self._async_slot_event is not None
            self._async_slot_event.clear()

        if at_limit:
            assert self._async_slot_event is not None
            try:
                await asyncio.wait_for(
                    self._async_slot_event.wait(),
                    timeout=effective_timeout,
                )
            except TimeoutError:
                return False
            # Re-acquire to claim the slot after event fires
            async with self._async_lock:
                if self._in_flight >= self._config.max_in_flight:
                    return False  # someone else grabbed the slot
                if self._async_rate_limiter is not None and not await self._async_rate_limiter.acquire():
                    if self._config.on_blocked == "raise":  # pragma: no cover
                        raise BackpressureError("Rate limit exceeded")  # pragma: no cover
                    return False
                self._in_flight += 1
                return True

        return False  # pragma: no cover

    async def release_async(self) -> None:
        """Async variant of ``release``."""
        self._ensure_async_primitives()
        assert self._async_lock is not None
        assert self._async_slot_event is not None
        async with self._async_lock:
            if self._in_flight > 0:
                self._in_flight -= 1
            self._async_slot_event.set()
