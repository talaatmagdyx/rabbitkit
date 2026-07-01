"""Handler timeout middleware.

Enforces a maximum processing time per message.  If the handler takes longer
than ``TimeoutConfig.timeout_seconds``, a ``HandlerTimeoutError`` is raised.

Implementation strategy
-----------------------
* **Async** — ``asyncio.wait_for()`` cancels the coroutine cleanly.
* **Sync**  — handler is run in a ``daemon=True`` ``threading.Thread``; the
  calling thread waits ``timeout_seconds`` and raises if the thread is still
  alive.  The background thread continues to its natural end but its result is
  discarded.  (CPython has no safe way to kill a thread, so long-running IO
  handlers will not be interrupted — they just become detached.)

``HandlerTimeoutError`` is a subclass of ``TimeoutError``.  The default error
classifier treats it as **TRANSIENT**, so retry middleware (if configured) will
re-queue the message.  Override the classifier if you want timeouts to go
straight to the DLQ.

Quick start
-----------
    from rabbitkit.middleware.timeout import TimeoutMiddleware, TimeoutConfig

    # 10-second hard limit on all handlers for this route
    timeout_mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=10.0))

    @broker.subscriber(queue="slow-tasks", middlewares=[timeout_mw])
    async def handle_task(body: bytes) -> None:
        await some_slow_operation()

Default timeout (30 s)::

    timeout_mw = TimeoutMiddleware()   # uses TimeoutConfig(timeout_seconds=30.0)

Combining with retry::

    from rabbitkit import RetryConfig
    from rabbitkit.middleware.retry import RetryMiddleware

    retry_mw = RetryMiddleware(RetryConfig(max_retries=3, delays=(5, 30, 120)))
    timeout_mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=15.0))

    # Order: timeout wraps handler, retry wraps timeout
    @broker.subscriber(
        queue="jobs",
        middlewares=[retry_mw, timeout_mw],   # retry outermost
    )
    async def run_job(body: bytes) -> None: ...

Exception hierarchy
-------------------
``HandlerTimeoutError(TimeoutError)``  — carries ``.timeout_seconds`` attribute
for logging / classification.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from rabbitkit.core.message import RabbitMessage
from rabbitkit.middleware.base import BaseMiddleware

logger = logging.getLogger(__name__)


class _DiscardedSettlement(BaseException):
    """H9 internal sentinel — raised (never caught by user code, since it's
    not an ``Exception``) from a guarded settlement stand-in to abort a
    discarded ack()/nack()/reject() call from an abandoned timed-out handler
    thread BEFORE ``RabbitMessage.ack()``/``nack()``/``reject()`` sets
    ``_disposition``. Without this, the discarded call would still flip
    ``_disposition`` away from "pending" (their guard for the real fn calls
    it AFTER the settlement fn returns, unconditionally) — silently
    preventing the consumer thread's own later, legitimate settlement from
    doing anything at all, since ``RabbitMessage`` would think the message
    was already settled. Landing back in ``_run()``'s ``except
    BaseException`` — harmless, since the background thread's outcome is
    already discarded by that point (see ``TimeoutMiddleware.consume_scope``)."""


class HandlerTimeoutError(TimeoutError):
    """Raised when a handler exceeds the configured timeout."""

    def __init__(self, timeout_seconds: float) -> None:
        super().__init__(f"Handler exceeded timeout of {timeout_seconds}s")
        self.timeout_seconds = timeout_seconds


@dataclass(frozen=True, slots=True)
class TimeoutConfig:
    """Configuration for handler timeout.

    Attributes:
        timeout_seconds: Maximum handler execution time in seconds.
    """

    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        if self.timeout_seconds <= 0:
            raise ValueError("timeout_seconds must be positive")


class TimeoutMiddleware(BaseMiddleware):
    """Enforces a maximum processing time per message.

    Async: uses ``asyncio.wait_for()`` — clean cancellation.
    Sync: runs ``call_next`` in a daemon ``threading.Thread`` and raises
    ``HandlerTimeoutError`` if the thread is still alive at the deadline.

    .. warning::
        CPython cannot safely kill a thread, so a sync handler that exceeds the
        timeout is **abandoned** — it keeps running detached until it finishes
        naturally. This can leak threads and resources for long-running / blocked
        IO handlers. Use an **async** handler for true cooperative cancellation.
        When a sync timeout fires, a CRITICAL log record is emitted and the
        optional ``on_timeout`` callback (if configured) is invoked so the
        abandonment is observable (e.g. increment a metric counter).
    """

    def __init__(
        self,
        config: TimeoutConfig | None = None,
        *,
        on_timeout: Callable[[RabbitMessage, float], None] | None = None,
    ) -> None:
        self._config = config or TimeoutConfig()
        self._on_timeout = on_timeout
        # Observable counter of sync threads abandoned due to timeout.
        self.abandoned_threads: int = 0

    def consume_scope(
        self,
        call_next: Any,
        message: RabbitMessage,
    ) -> Any:
        """Sync timeout — runs call_next in a thread with timeout.

        If the deadline elapses with the thread still alive, the thread is
        abandoned (CPython cannot kill it), a CRITICAL log is emitted, the
        ``abandoned_threads`` counter is incremented, ``on_timeout`` (if any)
        is invoked, and ``HandlerTimeoutError`` is raised.

        H9 — settlement is exclusively from the consumer (this) thread, never
        the background thread, even under ``AckPolicy.MANUAL`` (where the
        handler itself calls ``message.ack()``/``nack()``/``reject()``):
        while the background thread is running, ``message``'s settlement
        functions are swapped for stand-ins that CAPTURE (rather than
        execute) any settlement attempt made from that specific thread —
        calling the real pika-backed function from there, while THIS thread
        is blocked in ``thread.join()`` (i.e. not pumping the connection's
        I/O loop), can deadlock: the settlement call would marshal onto this
        thread and wait for it to drain the callback, but this thread is
        itself waiting on the background thread to finish. If the background
        thread finishes within the deadline, any settlement it captured is
        replayed for real on this (consumer/owner) thread, safely, after
        ``join()`` returns. If it does not, the guards stay installed
        (deliberately NOT restored — the background thread may still call
        ack()/nack()/reject() at any point later) but their thread-identity
        check means any FUTURE call specifically from that thread is
        discarded, while THIS thread's own subsequent settlement (e.g. via
        AckPolicy/RetryMiddleware after ``HandlerTimeoutError`` propagates
        below) is routed straight to the real fn — safe, since it's a
        same-thread call.
        """
        result_holder: list[Any] = []
        exception_holder: list[BaseException] = []
        captured_settlement: list[Callable[[], None]] = []
        timed_out = threading.Event()

        real_ack, real_nack, real_reject = message._ack_fn, message._nack_fn, message._reject_fn

        def _guarded_ack() -> None:
            if threading.get_ident() != thread.ident:
                if real_ack is not None:
                    real_ack()
                return
            if timed_out.is_set():
                logger.warning("Discarding ack() from an abandoned timed-out handler thread")
                # Raise (rather than return) so RabbitMessage.ack() does not
                # proceed to set _disposition="acked" for a call that never
                # actually touched the channel — that would silently block
                # the consumer thread's own later, real settlement.
                raise _DiscardedSettlement
            if real_ack is not None:
                captured_settlement.append(real_ack)

        def _guarded_nack(requeue: bool = True) -> None:
            if threading.get_ident() != thread.ident:
                if real_nack is not None:
                    real_nack(requeue)
                return
            if timed_out.is_set():
                logger.warning("Discarding nack() from an abandoned timed-out handler thread")
                raise _DiscardedSettlement
            if real_nack is not None:
                captured_settlement.append(lambda: real_nack(requeue))

        def _guarded_reject(requeue: bool = False) -> None:
            if threading.get_ident() != thread.ident:
                if real_reject is not None:
                    real_reject(requeue)
                return
            if timed_out.is_set():
                logger.warning("Discarding reject() from an abandoned timed-out handler thread")
                raise _DiscardedSettlement
            if real_reject is not None:
                captured_settlement.append(lambda: real_reject(requeue))

        # Only install a guard where a real fn exists — leaving an already-None
        # fn as None preserves message.ack()/nack()/reject()'s own "no
        # settlement fn set" RuntimeError for e.g. no-ack deliveries, instead
        # of silently swallowing it inside the guard.
        if real_ack is not None:
            message._ack_fn = _guarded_ack
        if real_nack is not None:
            message._nack_fn = _guarded_nack
        if real_reject is not None:
            message._reject_fn = _guarded_reject

        def _run() -> None:
            try:
                result_holder.append(call_next(message))
            except BaseException as exc:
                exception_holder.append(exc)

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        thread.join(timeout=self._config.timeout_seconds)

        if thread.is_alive():
            # CPython cannot safely kill a thread — the handler keeps running
            # detached. Make the abandonment explicit and observable.
            #
            # Deliberately do NOT restore the real settlement fns here: the
            # background thread is still running and may call
            # ack()/nack()/reject() at any point after this method returns —
            # if the real fn were restored, that eventual call would hit the
            # pika channel directly from a non-owner thread. The guards stay
            # installed; their threading.get_ident() check already routes
            # THIS thread's own subsequent settlement (AckPolicy/
            # RetryMiddleware, after the exception below propagates) straight
            # to the real fn (safe, same-thread), while any FUTURE call
            # specifically from the background thread is discarded now that
            # timed_out is set.
            timed_out.set()
            self.abandoned_threads += 1
            logger.critical(
                "Sync handler exceeded %.1fs timeout; thread abandoned (still running). "
                "Use an async handler for real cancellation. "
                "abandoned_threads=%d",
                self._config.timeout_seconds,
                self.abandoned_threads,
            )
            if self._on_timeout is not None:
                try:
                    self._on_timeout(message, self._config.timeout_seconds)
                except Exception:  # pragma: no cover - callback must not break flow
                    logger.exception("on_timeout callback raised")
            raise HandlerTimeoutError(self._config.timeout_seconds)

        # Finished within the deadline — restore the real fns, then replay
        # any settlement the background thread captured, for real, on this
        # (consumer/owner) thread.
        message._ack_fn, message._nack_fn, message._reject_fn = real_ack, real_nack, real_reject
        for replay in captured_settlement:
            replay()

        if exception_holder:
            raise exception_holder[0]

        return result_holder[0] if result_holder else None

    async def consume_scope_async(
        self,
        call_next: Any,
        message: RabbitMessage,
    ) -> Any:
        """Async timeout — uses asyncio.wait_for()."""
        try:
            return await asyncio.wait_for(
                call_next(message),
                timeout=self._config.timeout_seconds,
            )
        except TimeoutError:
            raise HandlerTimeoutError(self._config.timeout_seconds) from None
