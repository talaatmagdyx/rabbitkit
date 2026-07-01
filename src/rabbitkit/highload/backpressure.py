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

The ``on_blocked`` string is translated ONCE in ``FlowController.__init__``
into a ``_BlockedPolicy`` strategy, eliminating the stringly-typed
``if self._config.on_blocked == ...`` dispatch that was sprinkled through
``acquire`` / ``acquire_async``.
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
from typing import Protocol

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


# ── Blocked-policy strategies ────────────────────────────────────────────


# ``reason`` values passed to the policy strategies.  These are *semantic*
# inputs (not stringly-typed dispatch): each names the pressure signal that
# fired so the wait policy knows which primitive to block on.
_REASON_BLOCKED = "blocked"  # connection.blocked notification
_REASON_RATE = "rate"  # rate-limiter token unavailable
_REASON_SLOT = "slot"  # in-flight limit reached
_REASON_RATE_RETRY = "rate_retry"  # rate token gone after a slot wait (give up)


class _BlockedPolicy(Protocol):
    """Strategy encapsulating the ``on_blocked`` behaviour.

    ``handle`` / ``handle_async`` perform the full action (raise, drop, or
    block) and are called OUTSIDE the transport lock so the lock discipline
    (C-5) is preserved.  Returns ``True`` when the wait policy succeeds and
    the caller may proceed; ``False`` when the message should be dropped
    (drop policy, or wait-policy timeout); raises ``BackpressureError`` for
    the raise policy.
    """

    def handle(self, fc: FlowController, reason: str, timeout: float | None) -> bool: ...

    async def handle_async(self, fc: FlowController, reason: str, timeout: float | None) -> bool: ...


class _WaitPolicy:
    """Block until the pressure clears.  Returns False on timeout."""

    def handle(self, fc: FlowController, reason: str, timeout: float | None) -> bool:
        if reason == _REASON_BLOCKED:
            return fc._unblock_event.wait(timeout=timeout)
        if reason == _REASON_RATE:
            rl = fc._rate_limiter
            return rl is not None and rl.wait(timeout=timeout)
        if reason == _REASON_SLOT:
            return fc._slot_event.wait(timeout=timeout)
        # _REASON_RATE_RETRY — already waited for a slot; give up rather than
        # burn another rate-token wait within the same deadline.
        return False

    async def handle_async(self, fc: FlowController, reason: str, timeout: float | None) -> bool:
        if reason == _REASON_BLOCKED:
            assert fc._async_unblock_event is not None
            try:
                async with asyncio.timeout(timeout):
                    await fc._async_unblock_event.wait()
                return True
            except TimeoutError:
                return False
        if reason == _REASON_RATE:
            rl = fc._async_rate_limiter
            if rl is None:
                return False
            return await rl.wait(timeout=timeout)
        if reason == _REASON_SLOT:
            assert fc._async_slot_event is not None
            try:
                async with asyncio.timeout(timeout):
                    await fc._async_slot_event.wait()
                return True
            except TimeoutError:
                return False
        # _REASON_RATE_RETRY — give up (see sync counterpart).
        return False


class _RaisePolicy:
    """Raise ``BackpressureError`` immediately for any pressure signal."""

    def handle(self, fc: FlowController, reason: str, timeout: float | None) -> bool:
        raise BackpressureError(_raise_message(reason, fc))

    async def handle_async(self, fc: FlowController, reason: str, timeout: float | None) -> bool:
        raise BackpressureError(_raise_message(reason, fc))


class _DropPolicy:
    """Return False immediately — the caller should discard the message."""

    def handle(self, fc: FlowController, reason: str, timeout: float | None) -> bool:
        return False

    async def handle_async(self, fc: FlowController, reason: str, timeout: float | None) -> bool:
        return False


def _raise_message(reason: str, fc: FlowController) -> str:
    if reason == _REASON_BLOCKED:
        return "Connection is blocked by RabbitMQ"
    if reason == _REASON_SLOT:
        return f"In-flight limit reached ({fc._config.max_in_flight})"
    # _REASON_RATE / _REASON_RATE_RETRY
    return "Rate limit exceeded"


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

        # Select the on_blocked strategy ONCE (public API stays a string for
        # backward compat — the Strategy selection happens here).
        _strategies: dict[str, type[_BlockedPolicy]] = {
            "wait": _WaitPolicy,
            "raise": _RaisePolicy,
            "drop": _DropPolicy,
        }
        self._policy = _strategies[self._config.on_blocked]()

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

        With ``on_blocked="wait"`` a race loss (the slot we waited for was
        taken by another contender) re-loops instead of silently dropping —
        mirroring the async path (I-9). The loop is bounded by the deadline
        derived from *timeout* / ``blocked_timeout``.
        """
        effective_timeout = timeout if timeout is not None else self._config.blocked_timeout

        # 1. Check blocked state
        if self._blocked:
            if not self._policy.handle(self, _REASON_BLOCKED, effective_timeout):
                return False

        # 2. Check in-flight limit + rate limiter. The rate-limiter wait (which
        # sleeps) must happen OUTSIDE self._lock (C-5): under the lock we only do
        # non-blocking checks, then release the lock before any blocking wait.
        # On a race loss (the slot/token we waited for was taken by another
        # contender) we re-loop instead of dropping (I-9), bounded by deadline.
        deadline = None if effective_timeout is None else time.monotonic() + effective_timeout

        def _remaining() -> float | None:
            return None if deadline is None else max(0.0, deadline - time.monotonic())

        while True:
            rate_needed = False
            at_limit = False
            with self._lock:
                if self._in_flight < self._config.max_in_flight:
                    if self._rate_limiter is not None and not self._rate_limiter.acquire():
                        # No token right now; the (possibly blocking) wait happens
                        # outside the lock below. Fall through to the rate policy.
                        rate_needed = True
                    else:
                        self._in_flight += 1
                        if self._in_flight >= self._config.max_in_flight:
                            self._slot_event.clear()
                        return True
                else:
                    at_limit = True
                    self._slot_event.clear()

            if rate_needed:
                # Policy dispatch outside the lock (C-5): wait/raise/drop.
                if not self._policy.handle(self, _REASON_RATE, _remaining()):
                    return False
                # wait() consumed a token atomically; now claim the in-flight slot.
                with self._lock:
                    if self._in_flight < self._config.max_in_flight:
                        self._in_flight += 1
                        if self._in_flight >= self._config.max_in_flight:
                            self._slot_event.clear()
                        return True
                # The slot was taken while we waited for a token; wait for a slot
                # then re-loop and re-claim under the lock (I-9: was a silent drop).
                if not self._slot_event.wait(timeout=_remaining()):
                    return False
                continue

            if at_limit:
                # Policy dispatch outside the lock (C-5): wait/raise/drop.
                if not self._policy.handle(self, _REASON_SLOT, _remaining()):
                    return False
                # Re-loop and re-claim under the lock. If we lose the race again
                # (still at limit), the loop clears the event and re-waits — this
                # is the I-9 fix (previously we returned False on a single loss).
                continue

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
            if not await self._policy.handle_async(self, _REASON_BLOCKED, effective_timeout):
                return False

        # 2. Check in-flight limit + rate. Rate-token waits and in-flight slot
        # waits happen OUTSIDE the lock; on a race loss we re-wait (loop) instead
        # of dropping (H-P4). With on_blocked="wait" a missing rate token now
        # actually waits, mirroring the sync semantics.
        deadline = None if effective_timeout is None else time.monotonic() + effective_timeout

        def _remaining() -> float | None:
            return None if deadline is None else max(0.0, deadline - time.monotonic())

        while True:
            rate_needed = False
            at_limit = False
            async with self._async_lock:
                if self._in_flight < self._config.max_in_flight:
                    if self._async_rate_limiter is not None and not await self._async_rate_limiter.acquire():
                        # No token right now; the (possibly blocking) wait happens
                        # outside the lock below. Fall through to the rate policy.
                        rate_needed = True
                    else:
                        self._in_flight += 1
                        if self._in_flight >= self._config.max_in_flight:
                            assert self._async_slot_event is not None
                            self._async_slot_event.clear()
                        return True
                else:
                    at_limit = True
                    assert self._async_slot_event is not None
                    self._async_slot_event.clear()

            if rate_needed:
                # Policy dispatch outside the lock (C-5): wait/raise/drop.
                if not await self._policy.handle_async(self, _REASON_RATE, _remaining()):
                    return False
                # Token consumed by wait(); claim the slot without another acquire.
                async with self._async_lock:
                    if self._in_flight < self._config.max_in_flight:
                        self._in_flight += 1
                        if self._in_flight >= self._config.max_in_flight:
                            assert self._async_slot_event is not None
                            self._async_slot_event.clear()
                        return True
                # Slot was taken while we waited for a token: wait for a slot,
                # then retry with a NON-blocking rate acquire (don't burn another).
                assert self._async_slot_event is not None
                _rem = _remaining()
                if _rem is not None and _rem <= 0:
                    return False
                try:
                    async with asyncio.timeout(_rem):
                        await self._async_slot_event.wait()
                except TimeoutError:
                    return False
                async with self._async_lock:
                    # perf-M-1: re-loop instead of dropping (return False) so a
                    # caller that already paid for a rate token re-waits for a
                    # slot within the deadline (mirrors sync I-9 + at_limit path).
                    if self._in_flight >= self._config.max_in_flight:
                        continue
                    if self._async_rate_limiter is not None and not await self._async_rate_limiter.acquire():
                        # Rate token gone after the slot wait: drop (wait/drop) or
                        # raise. No re-loop here — we already waited for a slot.
                        if not await self._policy.handle_async(self, _REASON_RATE_RETRY, _remaining()):
                            return False
                    self._in_flight += 1
                    if self._in_flight >= self._config.max_in_flight:
                        assert self._async_slot_event is not None
                        self._async_slot_event.clear()
                    return True

            if at_limit:
                # Policy dispatch outside the lock (C-5): wait/raise/drop.
                assert self._async_slot_event is not None
                if not await self._policy.handle_async(self, _REASON_SLOT, _remaining()):
                    return False
                # Re-loop and re-claim under the lock. If we lose the race again
                # (still at limit), the loop clears the event and re-waits - this
                # is the H-P4 fix (previously we returned False on a single loss).
                continue

    async def release_async(self) -> None:
        """Async variant of ``release``."""
        self._ensure_async_primitives()
        assert self._async_lock is not None
        assert self._async_slot_event is not None
        async with self._async_lock:
            if self._in_flight > 0:
                self._in_flight -= 1
            self._async_slot_event.set()
