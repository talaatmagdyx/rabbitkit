"""Consumer concurrency -- multi-worker message processing.

Provides concurrent execution of message handlers within a single broker.

- Sync: ThreadPoolExecutor with configurable worker_count
- Async: asyncio.Semaphore limiting concurrent handler tasks

Compatible with all AckPolicy modes. Workers share the same transport
connection but process messages independently.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import threading
from collections.abc import Awaitable, Callable
from typing import Any

from rabbitkit.core.config import WorkerConfig
from rabbitkit.core.message import RabbitMessage

logger = logging.getLogger(__name__)


class SyncWorkerPool:
    """Thread pool for concurrent sync message processing.

    Wraps a handler callback to execute it in a thread pool with
    limited concurrency.

    Usage::

        pool = SyncWorkerPool(config=WorkerConfig(worker_count=4))
        pool.start()
        # Use pool.submit(callback, message) instead of callback(message)
        pool.stop()
    """

    def __init__(self, config: WorkerConfig | None = None) -> None:
        self._config = config or WorkerConfig()
        self._executor: concurrent.futures.ThreadPoolExecutor | None = None
        self._futures: set[concurrent.futures.Future[Any]] = set()
        self._futures_lock = threading.Lock()

    @property
    def worker_count(self) -> int:
        """Return the configured worker count."""
        return self._config.worker_count

    def start(self) -> None:
        """Start the worker pool."""
        if self._config.worker_count <= 1:
            return  # No pool needed for single worker
        self._executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self._config.worker_count,
            thread_name_prefix="rabbitkit-worker",
        )
        logger.info(
            "SyncWorkerPool started with %d workers", self._config.worker_count
        )

    def stop(self, timeout: float | None = None) -> None:
        """Stop the worker pool, waiting for in-flight tasks."""
        if self._executor is None:
            return
        effective = timeout if timeout is not None else self._config.stop_timeout
        with self._futures_lock:
            futures_snapshot = list(self._futures)
        _done, not_done = concurrent.futures.wait(futures_snapshot, timeout=effective)
        if not_done:
            logger.warning(
                "SyncWorkerPool: %d tasks did not complete within timeout",
                len(not_done),
            )
        self._executor.shutdown(wait=False)
        self._executor = None
        with self._futures_lock:
            self._futures.clear()
        logger.info("SyncWorkerPool stopped")

    def submit(
        self,
        callback: Callable[[RabbitMessage], None],
        message: RabbitMessage,
    ) -> None:
        """Submit a message for processing.

        If worker_count=1 (default), runs synchronously in the current thread.
        Otherwise, submits to the thread pool.
        """
        if self._executor is None:
            # Single worker mode -- run directly
            callback(message)
            return

        future = self._executor.submit(callback, message)
        with self._futures_lock:
            self._futures.add(future)
        # Self-cleaning: O(1) add + discard instead of rebuilding the list
        # on every message. Callback fires on the worker thread (or inline if
        # already done) and removes the future under the lock.
        future.add_done_callback(self._discard_future)

    def _discard_future(self, future: concurrent.futures.Future[Any]) -> None:
        with self._futures_lock:
            self._futures.discard(future)

    @property
    def pending_count(self) -> int:
        """Number of tasks currently pending/running."""
        if self._executor is None:
            return 0
        with self._futures_lock:
            return len(self._futures)


class AsyncWorkerPool:
    """Semaphore-based concurrent async message processing.

    Limits the number of concurrently processing async handlers.

    Usage::

        pool = AsyncWorkerPool(config=WorkerConfig(worker_count=4))
        pool.start()
        await pool.submit(callback, message)
        await pool.stop()
    """

    def __init__(self, config: WorkerConfig | None = None) -> None:
        self._config = config or WorkerConfig()
        self._semaphore: asyncio.Semaphore | None = None
        self._tasks: set[asyncio.Task[None]] = set()
        self._running = False

    @property
    def worker_count(self) -> int:
        """Return the configured worker count."""
        return self._config.worker_count

    def start(self) -> None:
        """Start the worker pool."""
        if self._config.worker_count <= 1:
            return
        self._semaphore = asyncio.Semaphore(self._config.worker_count)
        self._running = True
        logger.info(
            "AsyncWorkerPool started with %d workers", self._config.worker_count
        )

    async def stop(self, timeout: float | None = None) -> None:
        """Stop the worker pool, waiting for in-flight tasks."""
        self._running = False
        if not self._tasks:
            return
        effective = timeout if timeout is not None else self._config.stop_timeout
        _done, pending = await asyncio.wait(self._tasks, timeout=effective)
        for task in pending:
            task.cancel()
        self._tasks.clear()
        logger.info("AsyncWorkerPool stopped")

    async def submit(
        self,
        callback: Callable[[RabbitMessage], Awaitable[None]],
        message: RabbitMessage,
    ) -> None:
        """Submit a message for concurrent processing.

        If worker_count=1, runs directly (no semaphore).
        Otherwise, acquires semaphore slot before executing.
        """
        if self._semaphore is None:
            await callback(message)
            return

        semaphore = self._semaphore

        async def _run() -> None:
            async with semaphore:
                await callback(message)

        task = asyncio.create_task(_run())
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    @property
    def pending_count(self) -> int:
        """Number of tasks currently pending/running."""
        return len(self._tasks)
