"""Application lifecycle manager.

Lifecycle: on_startup → after_startup → [run] → on_shutdown → after_shutdown
Signal handling: SIGINT/SIGTERM → graceful shutdown
Idempotent: start() twice is safe (no-op if already running)
Startup failure rollback: if any startup hook fails → on_shutdown still called
State tracking: IDLE → STARTING → RUNNING → STOPPING → STOPPED
"""

from __future__ import annotations

import asyncio
import enum
import logging
import signal
from collections.abc import Callable
from typing import Any

logger = logging.getLogger(__name__)


class AppState(str, enum.Enum):
    """Application lifecycle states."""

    IDLE = "idle"
    STARTING = "starting"
    RUNNING = "running"
    STOPPING = "stopping"
    STOPPED = "stopped"


class RabbitApp:
    """Application lifecycle manager.

    Manages startup/shutdown hooks and signal handling.
    Idempotent: start() twice is safe (no-op if already running).
    Startup failure rollback: if any startup hook fails → on_shutdown still called.
    """

    def __init__(self, title: str = "rabbitkit") -> None:
        self._title = title
        self._state = AppState.IDLE

        # Lifecycle hooks
        self._on_startup: list[Callable[[], Any]] = []
        self._after_startup: list[Callable[[], Any]] = []
        self._on_shutdown: list[Callable[[], Any]] = []
        self._after_shutdown: list[Callable[[], Any]] = []

        # Signal handling
        self._shutdown_event: asyncio.Event | None = None
        self._original_handlers: dict[int, Any] = {}

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

        Idempotent — no-op if already running.
        On startup hook failure → on_shutdown is still called.
        """
        if self._state in (AppState.RUNNING, AppState.STARTING):
            logger.debug("App already %s, ignoring start()", self._state.value)
            return

        self._state = AppState.STARTING
        logger.info("Starting %s", self._title)

        try:
            self._run_hooks_sync(self._on_startup)
            self._state = AppState.RUNNING
            self._run_hooks_sync(self._after_startup)
            logger.info("%s started", self._title)
        except Exception:
            logger.exception("Startup failed, running shutdown hooks")
            self._state = AppState.STOPPING
            self._run_hooks_sync(self._on_shutdown)
            self._state = AppState.STOPPED
            raise

    def stop(self) -> None:
        """Stop the application (sync).

        Idempotent — no-op if already stopped.
        """
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

        Idempotent — no-op if already running.
        On startup hook failure → on_shutdown is still called.
        """
        if self._state in (AppState.RUNNING, AppState.STARTING):
            logger.debug("App already %s, ignoring start_async()", self._state.value)
            return

        self._state = AppState.STARTING
        logger.info("Starting %s", self._title)

        try:
            await self._run_hooks_async(self._on_startup)
            self._state = AppState.RUNNING
            await self._run_hooks_async(self._after_startup)
            logger.info("%s started", self._title)
        except Exception:
            logger.exception("Startup failed, running shutdown hooks")
            self._state = AppState.STOPPING
            await self._run_hooks_async(self._on_shutdown)
            self._state = AppState.STOPPED
            raise

    async def stop_async(self) -> None:
        """Stop the application (async).

        Idempotent — no-op if already stopped.
        """
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

        Installs SIGINT/SIGTERM handlers for graceful shutdown.
        """
        self._shutdown_event = asyncio.Event()

        # Install signal handlers
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._signal_handler)

        try:
            await self.start_async()
            await self._shutdown_event.wait()
        finally:
            await self.stop_async()
            # Restore signal handlers
            for sig in (signal.SIGINT, signal.SIGTERM):
                loop.remove_signal_handler(sig)

    def request_shutdown(self) -> None:
        """Request graceful shutdown (can be called from any context)."""
        if self._shutdown_event is not None:
            self._shutdown_event.set()

    # ── Internal ─────────────────────────────────────────────────────────

    def _signal_handler(self) -> None:
        """Handle SIGINT/SIGTERM."""
        logger.info("Received shutdown signal")
        self.request_shutdown()

    def _run_hooks_sync(self, hooks: list[Callable[[], Any]]) -> None:
        """Run hooks synchronously."""
        for hook in hooks:
            result = hook()
            if asyncio.iscoroutine(result):
                raise TypeError(
                    f"Async hook {hook.__qualname__} called in sync context. "
                    "Use start_async() or make the hook synchronous."
                )

    async def _run_hooks_async(self, hooks: list[Callable[[], Any]]) -> None:
        """Run hooks — supports both sync and async callables."""
        for hook in hooks:
            result = hook()
            if asyncio.iscoroutine(result):
                await result
