"""Consumer concurrency -- multi-worker message processing.

Provides concurrent execution of message handlers within a single broker.

- Sync: a daemon-thread worker pool with configurable worker_count
- Async: asyncio.Semaphore limiting concurrent handler tasks

Compatible with all AckPolicy modes. Workers share the same transport
connection but process messages independently.
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import logging
import queue
import threading
from collections.abc import Awaitable, Callable
from typing import Any

from rabbitkit.core.config import WorkerConfig
from rabbitkit.core.message import RabbitMessage

logger = logging.getLogger(__name__)


class _DaemonWorkerPool:
    """A bounded pool of DAEMON worker threads.

    ``concurrent.futures.ThreadPoolExecutor`` creates non-daemon threads, so a
    handler stuck in an uninterruptible call keeps the process alive past
    ``stop()`` until SIGKILL — bad for k8s graceful shutdown. This pool uses
    ``threading.Thread(daemon=True)`` workers, so the process can exit even if a
    worker is wedged, while still giving well-behaved handlers a bounded drain
    window via :meth:`shutdown`.

    Idle accounting: ``_idle_count`` counts workers that are *currently waiting
    for work* (not workers that are not running a task). It is incremented when a
    worker re-enters the wait and decremented when it picks up work, so
    ``_adjust_thread_count`` spawns a new worker only when no worker is idle —
    giving true ``max_workers`` parallelism under bursts (R-1/R-2 fix).
    """

    def __init__(self, max_workers: int, thread_name_prefix: str = "rabbitkit-worker") -> None:
        self._max_workers = max_workers
        self._thread_name_prefix = thread_name_prefix
        # (future, fn, args, kwargs)
        self._work: queue.Queue[
            tuple[concurrent.futures.Future[Any], Callable[..., Any], tuple[Any, ...], dict[str, Any]]
        ] = queue.Queue()
        self._threads: list[threading.Thread] = []
        self._shutdown = False
        self._idle_count = 0
        self._lock = threading.Lock()  # guards _idle_count, _threads, _shutdown reads

    def submit(self, fn: Callable[..., Any], /, *args: Any, **kwargs: Any) -> concurrent.futures.Future[Any]:
        with self._lock:
            if self._shutdown:
                raise RuntimeError("cannot schedule new futures after shutdown")
        fut: concurrent.futures.Future[Any] = concurrent.futures.Future()
        self._work.put((fut, fn, args, kwargs))
        self._adjust_thread_count()
        return fut

    def _adjust_thread_count(self) -> None:
        # Hold the lock across the whole check+spawn+append so concurrent
        # submit()s can't both pass the idle/needed checks and oversubscribe (R-2).
        with self._lock:
            idle = self._idle_count
            needed = self._max_workers - len(self._threads)
            if idle > 0 or needed <= 0 or self._shutdown:
                return  # an idle worker will pick it up, or we're at the cap / shutting down
            t = threading.Thread(
                target=self._worker,
                name=f"{self._thread_name_prefix}-{len(self._threads)}",
                daemon=True,
            )
            self._threads.append(t)
        t.start()

    def _worker(self) -> None:
        while True:
            # Mark idle BEFORE waiting so _adjust_thread_count sees an available worker
            # and doesn't oversubscribe. (R-1: this was the missing increment.)
            with self._lock:
                self._idle_count += 1
            try:
                try:
                    item = self._work.get(timeout=0.1)
                except queue.Empty:
                    with self._lock:
                        self._idle_count -= 1
                    if self._shutdown:
                        return
                    continue  # pragma: no cover — timing-dependent idle loop
                # Picked up work — no longer idle.
                with self._lock:
                    self._idle_count -= 1
            except BaseException:  # pragma: no cover - defensive
                with self._lock:
                    self._idle_count = max(0, self._idle_count - 1)
                raise
            fut, fn, args, kwargs = item
            if fut.set_running_or_notify_cancel():
                try:
                    fut.set_result(fn(*args, **kwargs))
                except BaseException as exc:
                    fut.set_exception(exc)
            # loop back: re-mark idle at the top of the next iteration

    def shutdown(self, wait: bool = False, cancel_futures: bool = False) -> None:
        with self._lock:
            self._shutdown = True
        if cancel_futures:
            # Drain queued work, marking each as cancelled.
            while True:
                try:
                    fut, _fn, _a, _k = self._work.get_nowait()
                except queue.Empty:
                    break
                fut.cancel()
        if wait:
            for t in self._threads:
                t.join(timeout=None)
        # Daemon threads: even if `wait=False` and some are stuck, the process
        # can still exit — they're daemon=True.

    @property
    def worker_count(self) -> int:
        with self._lock:
            return len(self._threads)


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
        self._executor: _DaemonWorkerPool | None = None
        self._futures: set[concurrent.futures.Future[Any]] = set()
        self._futures_lock = threading.Lock()

    @property
    def worker_count(self) -> int:
        """Return the configured worker count."""
        return self._config.worker_count

    def start(self) -> None:
        """Start the worker pool.

        Uses daemon worker threads (see :class:`_DaemonWorkerPool`) so a stuck
        handler cannot keep the process alive past ``stop()`` — important for
        k8s graceful shutdown where ``terminationGracePeriodSeconds`` must be
        honored without relying on SIGKILL.
        """
        if self._config.worker_count <= 1:
            return  # No pool needed for single worker
        self._executor = _DaemonWorkerPool(
            max_workers=self._config.worker_count,
            thread_name_prefix="rabbitkit-worker",
        )
        logger.info("SyncWorkerPool started with %d workers", self._config.worker_count)

    def stop(self, timeout: float | None = None) -> None:
        """Stop the worker pool, waiting for in-flight tasks.

        Cancels pending (not-yet-started) futures and bounds the wait for
        running ones by ``timeout`` (default ``WorkerConfig.stop_timeout``).
        Because workers are daemon threads, any task that does not finish in
        time is abandoned and the process can still exit cleanly — SIGKILL is
        no longer required as a backstop.
        """
        if self._executor is None:
            return
        effective = timeout if timeout is not None else self._config.stop_timeout
        with self._futures_lock:
            futures_snapshot = list(self._futures)
        _done, not_done = concurrent.futures.wait(futures_snapshot, timeout=effective)
        if not_done:
            logger.warning(
                "SyncWorkerPool: %d tasks did not complete within timeout; "
                "abandoning (daemon threads will not block process exit)",
                len(not_done),
            )
        # cancel_futures=True abandons queued-but-unstarted work; daemon threads
        # ensure the process exits even if a running handler is wedged.
        self._executor.shutdown(wait=False, cancel_futures=True)
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
        logger.info("AsyncWorkerPool started with %d workers", self._config.worker_count)

    async def stop(self, timeout: float | None = None) -> None:
        """Stop the worker pool, waiting for in-flight tasks.

        R-TaskGroup: rather than ``asyncio.wait`` + a manual ``task.cancel()``
        loop, we ``gather(*tasks, return_exceptions=True)`` bounded by
        ``asyncio.timeout`` (the 3.11+ idiom). Tasks that don't finish before
        the deadline are cancelled and awaited once more so their
        ``CancelledError`` is consumed rather than leaking as "Task was
        destroyed but it is pending" warnings.
        """
        self._running = False
        if not self._tasks:
            return
        effective = timeout if timeout is not None else self._config.stop_timeout
        tasks = list(self._tasks)
        try:
            async with asyncio.timeout(effective):
                await asyncio.gather(*tasks, return_exceptions=True)
        except TimeoutError:
            # Deadline elapsed: cancel any still-running tasks and drain them
            # so cancellation propagates cleanly (no pending-task warnings).
            not_done = [t for t in tasks if not t.done()]
            for task in not_done:  # pragma: no cover — gather already drains before raising
                task.cancel()  # pragma: no cover
            if not_done:  # pragma: no cover
                await asyncio.gather(*not_done, return_exceptions=True)  # pragma: no cover
        finally:
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
