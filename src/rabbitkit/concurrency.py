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
import contextvars
import logging
import queue
import threading
import time
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

    def __init__(
        self,
        max_workers: int,
        thread_name_prefix: str = "rabbitkit-worker",
        max_queue_size: int = 0,
    ) -> None:
        self._max_workers = max_workers
        self._thread_name_prefix = thread_name_prefix
        # (future, fn, args, kwargs). M11: maxsize=0 is unbounded (default);
        # >0 bounds the backlog. put() blocks when full — the caller (the pika
        # I/O thread) should keep max_queue_size >= prefetch so it never fills.
        self._work: queue.Queue[
            tuple[concurrent.futures.Future[Any], Callable[..., Any], tuple[Any, ...], dict[str, Any]]
        ] = queue.Queue(maxsize=max_queue_size)
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
                    # Run in a FRESH context per message.
                    #
                    # A pooled thread is long-lived and carries ONE
                    # contextvars.Context for its entire life, so calling the
                    # handler directly let message A's ContextVar writes leak
                    # into message B on the same thread. With DI's
                    # `set_local`, tenant B could read tenant A's scope --
                    # which contradicts the isolation the DI context class
                    # advertises. The async path is safe because each message
                    # is its own Task (and a Task already copies the context);
                    # this gives the sync pool the same guarantee.
                    fut.set_result(contextvars.copy_context().run(fn, *args, **kwargs))
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

    def __init__(self, config: WorkerConfig | None = None, *, threaded: bool = False) -> None:
        """``threaded=True`` runs handlers on worker threads even when
        ``worker_count`` is 1 (one dedicated thread instead of inline on the
        caller): ``SyncBroker`` uses it for ``ConsumerConfig.processing_timeout``,
        which needs the I/O thread free while a handler runs."""
        self._config = config or WorkerConfig()
        self._threaded = threaded
        self._executor: _DaemonWorkerPool | None = None
        self._futures: set[concurrent.futures.Future[Any]] = set()
        # H12: message associated with each in-flight future, so a future
        # abandoned at the stop_timeout deadline can be logged by delivery
        # tag/message id instead of just a bare count.
        self._future_messages: dict[concurrent.futures.Future[Any], RabbitMessage] = {}
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
        if self._config.worker_count <= 1 and not self._threaded:
            return  # No pool needed for single worker
        self._executor = _DaemonWorkerPool(
            max_workers=self._config.worker_count,
            thread_name_prefix="rabbitkit-worker",
            max_queue_size=self._config.max_queue_size,
        )
        logger.info("SyncWorkerPool started with %d workers", self._config.worker_count)

    def stop(self, timeout: float | None = None, pump: Callable[[], None] | None = None) -> None:
        """Stop the worker pool, waiting for in-flight tasks.

        Cancels pending (not-yet-started) futures and bounds the wait for
        running ones by ``timeout`` (default ``WorkerConfig.stop_timeout``).
        Because workers are daemon threads, any task that does not finish in
        time is abandoned and the process can still exit cleanly — SIGKILL is
        no longer required as a backstop.

        H2: when *pump* is given (``SyncBroker.stop()`` passes
        ``SyncTransport.pump``), the wait is polled in short slices with
        *pump* called between them, instead of one blocking
        ``concurrent.futures.wait(timeout=effective)``. A worker thread
        finishing its handler acks/nacks by marshaling onto the transport's
        owner thread via ``_run_on_io_thread`` — without something draining
        the connection's I/O loop while this method blocks, those marshaled
        callbacks would never run (they'd eventually time out on the worker
        side, or — before this fix — the transport fell back to an unsafe
        inline cross-thread call). *pump* must be safe to call from whichever
        thread calls ``stop()`` (i.e. ``stop()`` must run on the transport's
        owner thread when *pump* is given). Without *pump*, behavior is
        unchanged: a single blocking wait for the full *effective* timeout.
        """
        if self._executor is None:
            return
        effective = timeout if timeout is not None else self._config.stop_timeout
        with self._futures_lock:
            futures_snapshot = list(self._futures)
        if pump is None or not futures_snapshot:
            _done, not_done = concurrent.futures.wait(futures_snapshot, timeout=effective)
        else:
            poll = 0.05
            deadline = time.monotonic() + effective
            not_done = set(futures_snapshot)
            while not_done:
                pump()
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    break
                _done, not_done = concurrent.futures.wait(not_done, timeout=min(poll, remaining))
            pump()  # final drain in case the last ack was just marshaled
        if not_done:
            # H12: identify WHICH deliveries were abandoned (delivery_tag /
            # message_id), not just a count — the handler thread keeps
            # running (daemon, not killed) and may still ack/nack/produce
            # side effects later, so operators need to be able to correlate
            # an abandoned delivery with what shows up (or doesn't) downstream.
            with self._futures_lock:
                abandoned = [self._future_messages.get(f) for f in not_done]
            for message in abandoned:
                if message is None:
                    # Not tracked — e.g. a future submitted directly via the
                    # underlying executor rather than through submit().
                    continue
                logger.warning(
                    "SyncWorkerPool: handler for delivery_tag=%s message_id=%s did not "
                    "complete within stop_timeout; abandoning (its daemon thread keeps "
                    "running — ensure handlers are idempotent under at-least-once delivery)",
                    message.delivery_tag,
                    message.message_id,
                )
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
            self._future_messages.clear()
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
            # Single worker mode -- run directly, but still in a FRESH
            # context. This runs on the transport's owner thread, which lives
            # for the whole process, so without the copy every message's
            # ContextVar writes would accumulate on it and leak into the next
            # message exactly as they did on the pooled path.
            contextvars.copy_context().run(callback, message)
            return

        future = self._executor.submit(callback, message)
        with self._futures_lock:
            self._futures.add(future)
            self._future_messages[future] = message
        # Self-cleaning: O(1) add + discard instead of rebuilding the list
        # on every message. Callback fires on the worker thread (or inline if
        # already done) and removes the future under the lock.
        future.add_done_callback(self._discard_future)

    def _discard_future(self, future: concurrent.futures.Future[Any]) -> None:
        with self._futures_lock:
            self._futures.discard(future)
            self._future_messages.pop(future, None)

    @property
    def pending_count(self) -> int:
        """Number of tasks currently pending/running."""
        if self._executor is None:
            return 0
        with self._futures_lock:
            return len(self._futures)


#: How long AsyncWorkerPool.stop() waits for cancelled tasks to unwind after
#: its deadline before abandoning them (keeps stop() bounded).
_CANCEL_JOIN_TIMEOUT = 1.0


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
        # H12: message behind each task, so a task abandoned at the
        # stop_timeout deadline can be nacked (redelivery) and logged by
        # delivery tag/message id instead of just cancelled and forgotten.
        self._task_messages: dict[asyncio.Task[None], RabbitMessage] = {}
        # Tasks that hold a worker slot (their handler has started). The rest
        # of ``_tasks`` are prefetched deliveries still waiting for a slot;
        # stop() releases those at once instead of running them (§3.2).
        self._started: set[asyncio.Task[None]] = set()
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

        Deliveries that were prefetched into the pool but are still waiting
        for a worker slot are released first: their tasks are cancelled
        before the handler starts and each message is nacked with
        ``requeue=True``, so the broker can hand it to another consumer at
        once instead of this process running it during the grace period.

        Then waits up to *timeout* for in-flight tasks, then cancels the rest and
        waits at most ``_CANCEL_JOIN_TIMEOUT`` more for them to unwind. A
        handler that swallows ``CancelledError`` is abandoned (logged) rather
        than allowed to hold ``stop()`` open past its deadline.

        H12: a task cancelled by the deadline is *not* guaranteed to have
        reached its own ``ack``/``nack`` — ``CancelledError`` is a
        ``BaseException``, so it skips right past the pipeline's
        ``except Exception`` handling and the message is left unsettled.
        Rather than leave that to the implicit requeue-on-unacked-channel-close
        behavior when the transport eventually disconnects, any message still
        unsettled after a task is abandoned is nacked (requeue) here and
        logged by delivery tag — an explicit, immediate, observable
        redelivery instead of an implicit one.
        """
        self._running = False
        if not self._tasks:
            return
        effective = timeout if timeout is not None else self._config.stop_timeout
        task_messages = dict(self._task_messages)
        try:
            await self._release_queued(task_messages)
            tasks = [task for task in self._tasks if task in self._started]
            if not tasks:
                return
            # asyncio.wait, not gather: a cancelled gather only completes once
            # every child has finished, so one handler that swallows
            # CancelledError made stop() unbounded (INV-008).
            _done, pending = await asyncio.wait(tasks, timeout=effective)
            if not pending:
                return
            for task in pending:
                task.cancel()
            _done, stuck = await asyncio.wait(pending, timeout=_CANCEL_JOIN_TIMEOUT)
            for task in stuck:
                message = task_messages.get(task)
                logger.error(
                    "AsyncWorkerPool: handler for delivery_tag=%s message_id=%s ignored cancellation "
                    "for %.1fs; abandoning it (it may still run, and its side effects may still happen)",
                    getattr(message, "delivery_tag", None),
                    getattr(message, "message_id", None),
                    _CANCEL_JOIN_TIMEOUT,
                )
            for task in pending:
                message = task_messages.get(task)
                if message is None or message.is_settled:
                    continue  # handler reached its own ack/nack before being cut off
                logger.warning(
                    "AsyncWorkerPool: handler for delivery_tag=%s message_id=%s did not "
                    "complete within stop_timeout; abandoning (task cancelled) and nacking "
                    "for redelivery — ensure handlers are idempotent under at-least-once delivery",
                    message.delivery_tag,
                    message.message_id,
                )
                try:
                    await message.nack_async(requeue=True)
                except Exception:
                    logger.warning("nack on abandoned handler's message raised", exc_info=True)
        finally:
            self._tasks.clear()
            self._task_messages.clear()
            self._started.clear()
            logger.info("AsyncWorkerPool stopped")

    async def _release_queued(self, task_messages: dict[asyncio.Task[None], RabbitMessage]) -> None:
        """Cancel tasks still waiting for a slot and nack their messages (requeue)."""
        queued = [task for task in self._tasks if task not in self._started]
        if not queued:
            return
        for task in queued:
            task.cancel()
        # A task waiting on the semaphore unwinds at its next step; the bound
        # only guards against a callback that is not ours.
        await asyncio.wait(queued, timeout=_CANCEL_JOIN_TIMEOUT)
        released = 0
        for task in queued:
            message = task_messages.get(task)
            if message is None or message.is_settled:
                continue
            try:
                await message.nack_async(requeue=True)
                released += 1
            except Exception:
                logger.warning(
                    "AsyncWorkerPool: nack of not-started delivery_tag=%s raised; the broker "
                    "redelivers it when the channel closes",
                    message.delivery_tag,
                    exc_info=True,
                )
        logger.info(
            "AsyncWorkerPool: released %d prefetched message(s) that had not started (nack requeue=True)",
            released,
        )

    async def submit(
        self,
        callback: Callable[[RabbitMessage], Awaitable[None]],
        message: RabbitMessage,
    ) -> None:
        """Submit a message for concurrent processing.

        If worker_count=1, runs directly (no semaphore).
        Otherwise, acquires semaphore slot before executing.

        H12: refuses (nacks for redelivery) instead of scheduling an unawaited
        task when the pool isn't running — e.g. a delivery callback firing
        after ``stop()`` already cleared ``_tasks``. Nothing would ever await
        that task, so it would silently race the event loop's own shutdown
        instead of the message being cleanly settled.
        """
        if self._semaphore is None:
            await callback(message)
            return

        if not self._running:
            logger.warning(
                "AsyncWorkerPool.submit() called while stopped (delivery_tag=%s, "
                "message_id=%s); nacking for redelivery instead of orphaning an "
                "unawaited task",
                message.delivery_tag,
                message.message_id,
            )
            if not message.is_settled:
                await message.nack_async(requeue=True)
            return

        semaphore = self._semaphore

        started = self._started

        async def _run() -> None:
            async with semaphore:
                task = asyncio.current_task()
                if task is not None:
                    started.add(task)
                await callback(message)

        task = asyncio.create_task(_run())
        self._tasks.add(task)
        self._task_messages[task] = message
        task.add_done_callback(self._on_task_done)

    def _on_task_done(self, task: asyncio.Task[None]) -> None:
        self._tasks.discard(task)
        self._task_messages.pop(task, None)
        self._started.discard(task)

    @property
    def pending_count(self) -> int:
        """Number of tasks currently pending/running."""
        return len(self._tasks)
