"""Application lifecycle manager.

Lifecycle: on_startup → after_startup → [run] → on_shutdown → after_shutdown
Signal handling: SIGINT/SIGTERM → graceful shutdown
Idempotent: start() twice is safe (no-op if already running)
Startup failure rollback: if any startup hook fails → on_shutdown still called
State tracking: IDLE → STARTING → RUNNING → STOPPING → STOPPED
"""

from __future__ import annotations

import asyncio
import logging
import signal
import threading
from collections.abc import Callable
from typing import Any

from rabbitkit.core.types import AppState

logger = logging.getLogger(__name__)

# ``AppState`` is re-exported here for backwards compatibility; its canonical home
# is ``rabbitkit.core.types`` (single canonical location for all enums).
__all__ = ["AppState", "RabbitApp"]


class RabbitApp:
    """Application lifecycle manager.

    Manages startup/shutdown hooks and signal handling.
    Idempotent: start() twice is safe (no-op if already running).
    Startup failure rollback: if any startup hook fails → on_shutdown still called.

    ``startup_timeout`` bounds each startup/after-startup hook so a hung hook
    fails fast instead of hanging until SIGKILL. The default (``120.0``) is
    finite; pass ``None`` explicitly to disable the bound.
    """

    def __init__(self, title: str = "rabbitkit", *, startup_timeout: float | None = 120.0) -> None:
        self._title = title
        self._state = AppState.IDLE
        self._startup_timeout = startup_timeout

        # Lifecycle hooks
        self._on_startup: list[Callable[[], Any]] = []
        self._after_startup: list[Callable[[], Any]] = []
        self._on_shutdown: list[Callable[[], Any]] = []
        self._after_shutdown: list[Callable[[], Any]] = []

        # Concurrency guards: prevent double-start / concurrent stop races.
        # The async lock is created eagerly — modern asyncio binds it to the
        # running loop on first use, so creating it without a loop is safe and
        # avoids a lazy-create race in concurrent start_async/stop_async calls.
        self._sync_lock = threading.Lock()
        self._async_lock = asyncio.Lock()

        # Signal handling
        self._shutdown_event: asyncio.Event | None = None
        self._original_handlers: dict[signal.Signals, Any] = {}
        self._loop: asyncio.AbstractEventLoop | None = None

    @property
    def state(self) -> AppState:
        return self._state

    @property
    def title(self) -> str:
        return self._title

    # ── Hook registration ────────────────────────────────────────────────

    def on_startup(self, func: Callable[[], Any]) -> Callable[[], Any]:
        """Register a startup hook (decorator or direct call)."""
        self._on_startup.append(func)
        return func

    def after_startup(self, func: Callable[[], Any]) -> Callable[[], Any]:
        """Register a post-startup hook."""
        self._after_startup.append(func)
        return func

    def on_shutdown(self, func: Callable[[], Any]) -> Callable[[], Any]:
        """Register a shutdown hook."""
        self._on_shutdown.append(func)
        return func

    def after_shutdown(self, func: Callable[[], Any]) -> Callable[[], Any]:
        """Register a post-shutdown hook."""
        self._after_shutdown.append(func)
        return func

    # ── Sync lifecycle ───────────────────────────────────────────────────

    def start(self) -> None:
        """Start the application (sync).

        Idempotent and concurrency-safe — no-op if already running.
        On startup hook failure → on_shutdown is still called.
        """
        with self._sync_lock:
            if self._state in (AppState.RUNNING, AppState.STARTING, AppState.STOPPING):
                logger.debug("App already %s, ignoring start()", self._state.value)
                return

            self._state = AppState.STARTING
            logger.info("Starting %s", self._title)

            try:
                self._run_hooks_sync(self._on_startup, timeout=self._startup_timeout)
                self._state = AppState.RUNNING
                self._run_hooks_sync(self._after_startup, timeout=self._startup_timeout)
                logger.info("%s started", self._title)
            except Exception:
                logger.exception("Startup failed, running shutdown hooks")
                self._state = AppState.STOPPING
                self._run_hooks_sync(self._on_shutdown)
                self._state = AppState.STOPPED
                raise

    def stop(self) -> None:
        """Stop the application (sync).

        Idempotent and concurrency-safe — no-op if already stopped.
        """
        with self._sync_lock:
            if self._state in (AppState.STOPPED, AppState.STOPPING, AppState.IDLE):
                return

            self._state = AppState.STOPPING
            logger.info("Stopping %s", self._title)

            try:
                self._run_hooks_sync(self._on_shutdown)
            finally:
                self._run_hooks_sync(self._after_shutdown)
                self._state = AppState.STOPPED
                logger.info("%s stopped", self._title)

    # ── Async lifecycle ──────────────────────────────────────────────────

    async def start_async(self) -> None:
        """Start the application (async).

        Idempotent and concurrency-safe — no-op if already running.
        On startup hook failure → on_shutdown is still called.
        """
        async with self._async_lock:
            if self._state in (AppState.RUNNING, AppState.STARTING, AppState.STOPPING):
                logger.debug("App already %s, ignoring start_async()", self._state.value)
                return

            self._state = AppState.STARTING
            logger.info("Starting %s", self._title)

            try:
                await self._run_hooks_async(self._on_startup, timeout=self._startup_timeout)
                self._state = AppState.RUNNING
                await self._run_hooks_async(self._after_startup, timeout=self._startup_timeout)
                logger.info("%s started", self._title)
            except Exception:
                logger.exception("Startup failed, running shutdown hooks")
                self._state = AppState.STOPPING
                await self._run_hooks_async(self._on_shutdown)
                self._state = AppState.STOPPED
                raise

    async def stop_async(self) -> None:
        """Stop the application (async).

        Idempotent and concurrency-safe — no-op if already stopped.
        """
        async with self._async_lock:
            if self._state in (AppState.STOPPED, AppState.STOPPING, AppState.IDLE):
                return

            self._state = AppState.STOPPING
            logger.info("Stopping %s", self._title)

            try:
                await self._run_hooks_async(self._on_shutdown)
            finally:
                await self._run_hooks_async(self._after_shutdown)
                self._state = AppState.STOPPED
                logger.info("%s stopped", self._title)

    async def run_async(self) -> None:
        """Start, wait for shutdown signal, then stop (async).

        Installs SIGINT/SIGTERM handlers for graceful shutdown. Falls back to
        ``signal.signal`` on platforms/threads where ``loop.add_signal_handler``
        is unavailable (e.g. Windows, non-main-thread).
        """
        self._shutdown_event = asyncio.Event()

        loop = asyncio.get_running_loop()
        self._loop = loop
        installed = False
        try:
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.add_signal_handler(sig, self._signal_handler)
            installed = True
        except (NotImplementedError, RuntimeError, ValueError):  # pragma: no cover
            # Not supported on this platform/thread — fall back to signal.signal.
            # The handler runs in a signal context; schedule the set via the loop.
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    self._original_handlers[sig] = signal.signal(sig, self._signal_handler_sync)
                except (ValueError, OSError):  # pragma: no cover
                    pass  # not in main thread — best effort

        try:
            await self.start_async()
            await self._shutdown_event.wait()
        finally:
            await self.stop_async()
            # Restore signal handlers
            if installed:
                for sig in (signal.SIGINT, signal.SIGTERM):
                    try:
                        loop.remove_signal_handler(sig)
                    except (NotImplementedError, RuntimeError, ValueError):  # pragma: no cover
                        pass
            else:  # pragma: no cover
                for sig, prev in self._original_handlers.items():
                    try:
                        signal.signal(sig, prev)
                    except (ValueError, OSError):  # pragma: no cover
                        pass
                self._original_handlers.clear()

    def _signal_handler_sync(self, signum: int, frame: Any) -> None:  # pragma: no cover
        """signal.signal fallback handler — schedule shutdown on the loop."""
        logger.info("Received shutdown signal %d", signum)
        if self._shutdown_event is not None and self._loop is not None:
            self._loop.call_soon_threadsafe(self._shutdown_event.set)

    def request_shutdown(self) -> None:
        """Request graceful shutdown (can be called from any context)."""
        if self._shutdown_event is not None:
            self._shutdown_event.set()

    # ── Internal ─────────────────────────────────────────────────────────

    def _signal_handler(self) -> None:
        """Handle SIGINT/SIGTERM."""
        logger.info("Received shutdown signal")
        self.request_shutdown()

    def _run_hooks_sync(self, hooks: list[Callable[[], Any]], *, timeout: float | None = None) -> None:
        """Run hooks synchronously. If ``timeout`` is set, bound each hook.

        When a timeout is set, each hook runs in a dedicated worker thread so
        the caller is not blocked by a hung hook. NOTE: the worker thread is
        non-daemon, so a truly-uninterruptible hook still lingers until process
        exit; the timeout bounds the *caller* (it receives ``TimeoutError``)
        rather than hanging forever. ``cancel_futures=True`` drops any
        not-yet-started work.

        The executor is NOT used as a context manager — its ``__exit__`` calls
        ``shutdown(wait=True)`` which would block forever on a hung hook.
        """
        import concurrent.futures

        for hook in hooks:
            if timeout is None:
                result = hook()
                if asyncio.iscoroutine(result):
                    result.close()
                    raise TypeError(
                        f"Async hook {hook.__qualname__} called in sync context. "
                        "Use start_async() or make the hook synchronous."
                    )
            else:
                ex = concurrent.futures.ThreadPoolExecutor(max_workers=1)
                try:
                    fut = ex.submit(hook)
                    try:
                        result = fut.result(timeout=timeout)
                    except concurrent.futures.TimeoutError as e:
                        raise TimeoutError(f"Startup hook {hook.__qualname__} exceeded timeout {timeout}s") from e
                    if asyncio.iscoroutine(result):
                        raise TypeError(
                            f"Async hook {hook.__qualname__} called in sync context. "
                            "Use start_async() or make the hook synchronous."
                        )
                finally:
                    # wait=False so a hung worker thread does not block the caller;
                    # cancel_futures=True drops any queued (not-yet-started) work.
                    ex.shutdown(wait=False, cancel_futures=True)

    async def _run_hooks_async(self, hooks: list[Callable[[], Any]], *, timeout: float | None = None) -> None:
        """Run hooks — supports both sync and async callables. Bounds each hook.

        With a timeout: coroutine-function hooks are awaited bounded; sync
        hooks run in a worker thread via ``asyncio.to_thread`` bounded (a sync
        hook running inline would be unbounded). If a sync hook returns a
        future, that future is awaited bounded as well.
        """
        for hook in hooks:
            if timeout is None:
                result = hook()
                if asyncio.iscoroutine(result):
                    await result
            elif asyncio.iscoroutinefunction(hook):
                result = hook()  # builds the coroutine; body not yet executed
                try:
                    async with asyncio.timeout(timeout):
                        await result
                except TimeoutError as e:
                    raise TimeoutError(f"Startup hook {hook.__qualname__} exceeded timeout {timeout}s") from e
            else:
                # Sync hook — run in a thread so it is bounded by the timeout.
                try:
                    result = await asyncio.wait_for(asyncio.to_thread(hook), timeout=timeout)
                except TimeoutError as e:
                    raise TimeoutError(f"Startup hook {hook.__qualname__} exceeded timeout {timeout}s") from e
                if asyncio.isfuture(result):
                    try:
                        async with asyncio.timeout(timeout):
                            await result
                    except TimeoutError as e:
                        raise TimeoutError(f"Startup hook {hook.__qualname__} exceeded timeout {timeout}s") from e
