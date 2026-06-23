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
import threading
from dataclasses import dataclass
from typing import Any

from rabbitkit.core.message import RabbitMessage
from rabbitkit.middleware.base import BaseMiddleware


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

    Async: uses asyncio.wait_for() — clean cancellation.
    Sync: uses concurrent.futures with timeout — raises HandlerTimeoutError.
    """

    def __init__(self, config: TimeoutConfig | None = None) -> None:
        self._config = config or TimeoutConfig()

    def consume_scope(
        self,
        call_next: Any,
        message: RabbitMessage,
    ) -> Any:
        """Sync timeout — runs call_next in a thread with timeout."""
        result_holder: list[Any] = []
        exception_holder: list[BaseException] = []

        def _run() -> None:
            try:
                result_holder.append(call_next(message))
            except BaseException as exc:
                exception_holder.append(exc)

        thread = threading.Thread(target=_run, daemon=True)
        thread.start()
        thread.join(timeout=self._config.timeout_seconds)

        if thread.is_alive():
            raise HandlerTimeoutError(self._config.timeout_seconds)

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
