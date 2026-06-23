"""Tests for concurrency.py -- SyncWorkerPool and AsyncWorkerPool."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

from rabbitkit.concurrency import AsyncWorkerPool, SyncWorkerPool
from rabbitkit.core.config import WorkerConfig
from rabbitkit.core.message import RabbitMessage


def _make_message(**kwargs: Any) -> RabbitMessage:
    defaults: dict[str, Any] = {
        "body": b"test",
        "routing_key": "test.key",
        "headers": {},
        "path": {},
    }
    defaults.update(kwargs)
    return RabbitMessage(**defaults)


# ── SyncWorkerPool ────────────────────────────────────────────────────────


class TestSyncWorkerPool:
    def test_single_worker_runs_directly(self) -> None:
        """worker_count=1 runs callback in current thread."""
        pool = SyncWorkerPool(WorkerConfig(worker_count=1))
        pool.start()

        thread_ids: list[int | None] = []

        def callback(msg: RabbitMessage) -> None:
            thread_ids.append(threading.current_thread().ident)

        pool.submit(callback, _make_message())
        pool.stop()

        assert len(thread_ids) == 1
        assert thread_ids[0] == threading.current_thread().ident

    def test_multi_worker_uses_thread_pool(self) -> None:
        """worker_count>1 submits to thread pool."""
        pool = SyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()

        thread_ids: list[int | None] = []
        barrier = threading.Barrier(2, timeout=5)

        def callback(msg: RabbitMessage) -> None:
            thread_ids.append(threading.current_thread().ident)
            barrier.wait()

        msg1 = _make_message()
        msg2 = _make_message()
        pool.submit(callback, msg1)
        pool.submit(callback, msg2)

        pool.stop(timeout=5.0)

        assert len(thread_ids) == 2
        # Both ran in worker threads, not main thread
        main_id = threading.current_thread().ident
        assert all(tid != main_id for tid in thread_ids)

    def test_default_config(self) -> None:
        """Default config uses worker_count=1."""
        pool = SyncWorkerPool()
        assert pool.worker_count == 1

    def test_stop_without_start(self) -> None:
        """Stopping a pool that was never started does not raise."""
        pool = SyncWorkerPool()
        pool.stop()  # no error

    def test_pending_count_no_executor(self) -> None:
        """pending_count returns 0 when no executor exists."""
        pool = SyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()
        assert pool.pending_count == 0
        pool.stop()

    def test_start_with_single_worker_no_executor(self) -> None:
        """Single worker does not create an executor."""
        pool = SyncWorkerPool(WorkerConfig(worker_count=1))
        pool.start()
        assert pool._executor is None
        pool.stop()

    def test_pending_count_with_active_tasks(self) -> None:
        """pending_count reflects actively running tasks."""
        pool = SyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()

        event = threading.Event()

        def blocking_callback(msg: RabbitMessage) -> None:
            event.wait(timeout=5)

        pool.submit(blocking_callback, _make_message())
        # Task should be pending
        assert pool.pending_count >= 1

        event.set()
        pool.stop(timeout=5.0)

    def test_completed_futures_cleaned_on_submit(self) -> None:
        """Completed futures are removed from the list on submit."""
        pool = SyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()

        called: list[bool] = []

        def fast_callback(msg: RabbitMessage) -> None:
            called.append(True)

        pool.submit(fast_callback, _make_message())
        # Give thread pool time to complete
        time.sleep(0.1)
        # Next submit triggers cleanup
        pool.submit(fast_callback, _make_message())
        time.sleep(0.1)

        assert len(called) == 2
        pool.stop()


# ── AsyncWorkerPool ───────────────────────────────────────────────────────


class TestAsyncWorkerPool:
    async def test_single_worker_runs_directly(self) -> None:
        """worker_count=1 runs callback directly without semaphore."""
        pool = AsyncWorkerPool(WorkerConfig(worker_count=1))
        pool.start()

        called: list[bool] = []

        async def callback(msg: RabbitMessage) -> None:
            called.append(True)

        await pool.submit(callback, _make_message())
        await pool.stop()
        assert called == [True]

    async def test_multi_worker_concurrent(self) -> None:
        """Multiple messages are processed concurrently."""
        pool = AsyncWorkerPool(WorkerConfig(worker_count=3))
        pool.start()

        active: list[int] = []
        max_concurrent: list[int] = [0]
        lock = asyncio.Lock()

        async def callback(msg: RabbitMessage) -> None:
            async with lock:
                active.append(1)
                current = len(active)
                if current > max_concurrent[0]:
                    max_concurrent[0] = current

            await asyncio.sleep(0.05)

            async with lock:
                active.pop()

        # Submit 3 messages
        for _ in range(3):
            await pool.submit(callback, _make_message())

        # Wait for tasks to complete
        await asyncio.sleep(0.2)
        await pool.stop()

        assert max_concurrent[0] >= 2  # at least 2 ran concurrently

    async def test_semaphore_limits_concurrency(self) -> None:
        """Semaphore limits concurrent execution to worker_count."""
        pool = AsyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()

        max_concurrent: list[int] = [0]
        current_count: list[int] = [0]
        lock = asyncio.Lock()

        async def callback(msg: RabbitMessage) -> None:
            async with lock:
                current_count[0] += 1
                if current_count[0] > max_concurrent[0]:
                    max_concurrent[0] = current_count[0]
            await asyncio.sleep(0.05)
            async with lock:
                current_count[0] -= 1

        for _ in range(5):
            await pool.submit(callback, _make_message())

        await asyncio.sleep(0.5)
        await pool.stop()

        assert max_concurrent[0] <= 2

    async def test_default_config(self) -> None:
        """Default config uses worker_count=1."""
        pool = AsyncWorkerPool()
        assert pool.worker_count == 1

    async def test_stop_without_start(self) -> None:
        """Stopping a pool that was never started does not raise."""
        pool = AsyncWorkerPool()
        await pool.stop()  # no error

    async def test_pending_count_empty(self) -> None:
        """pending_count returns 0 when no tasks exist."""
        pool = AsyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()
        assert pool.pending_count == 0
        await pool.stop()

    async def test_start_single_worker_no_semaphore(self) -> None:
        """Single worker does not create a semaphore."""
        pool = AsyncWorkerPool(WorkerConfig(worker_count=1))
        pool.start()
        assert pool._semaphore is None
        await pool.stop()

    async def test_pending_count_with_active_tasks(self) -> None:
        """pending_count reflects in-flight tasks."""
        pool = AsyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()

        event = asyncio.Event()

        async def blocking_callback(msg: RabbitMessage) -> None:
            await event.wait()

        await pool.submit(blocking_callback, _make_message())
        # Give the task a moment to start
        await asyncio.sleep(0.01)
        assert pool.pending_count >= 1

        event.set()
        await pool.stop(timeout=5.0)

    async def test_stop_cancels_pending_on_timeout(self) -> None:
        """Tasks that exceed the stop timeout are cancelled."""
        pool = AsyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()

        async def never_finish(msg: RabbitMessage) -> None:
            await asyncio.sleep(100)

        await pool.submit(never_finish, _make_message())
        await asyncio.sleep(0.01)

        # Stop with very short timeout -- task should be cancelled
        await pool.stop(timeout=0.05)
        assert pool.pending_count == 0

    async def test_done_callback_removes_task(self) -> None:
        """Completed tasks are removed from the task set automatically."""
        pool = AsyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()

        async def quick_callback(msg: RabbitMessage) -> None:
            pass

        await pool.submit(quick_callback, _make_message())
        # Wait for task to complete
        await asyncio.sleep(0.05)
        assert pool.pending_count == 0
        await pool.stop()


# ── WorkerConfig.stop_timeout ────────────────────────────────────────────


class TestWorkerConfigStopTimeout:
    def test_sync_stop_uses_config_timeout(self) -> None:
        """SyncWorkerPool.stop() uses WorkerConfig.stop_timeout as default."""
        import concurrent.futures
        from unittest.mock import patch

        pool = SyncWorkerPool(config=WorkerConfig(worker_count=2, stop_timeout=5.0))
        pool.start()

        wait_timeouts: list[float | None] = []
        original_wait = concurrent.futures.wait

        def recording_wait(
            fs: object, timeout: float | None = None, **kw: object
        ) -> object:
            wait_timeouts.append(timeout)
            return original_wait(fs, timeout=timeout, **kw)  # type: ignore[arg-type]

        with patch("rabbitkit.concurrency.concurrent.futures.wait", side_effect=recording_wait):
            pool.stop()

        assert wait_timeouts == [5.0]

    def test_sync_stop_explicit_timeout_overrides_config(self) -> None:
        """Explicit timeout arg to stop() overrides WorkerConfig.stop_timeout."""
        import concurrent.futures
        from unittest.mock import patch

        pool = SyncWorkerPool(config=WorkerConfig(worker_count=2, stop_timeout=999.0))
        pool.start()

        wait_timeouts: list[float | None] = []
        original_wait = concurrent.futures.wait

        def recording_wait(
            fs: object, timeout: float | None = None, **kw: object
        ) -> object:
            wait_timeouts.append(timeout)
            return original_wait(fs, timeout=timeout, **kw)  # type: ignore[arg-type]

        with patch("rabbitkit.concurrency.concurrent.futures.wait", side_effect=recording_wait):
            pool.stop(timeout=7.0)

        assert wait_timeouts == [7.0]

    def test_default_stop_timeout_is_30(self) -> None:
        """Default stop_timeout is 30 seconds."""
        from rabbitkit.core.config import WorkerConfig

        assert WorkerConfig().stop_timeout == 30.0

    async def test_async_stop_uses_config_timeout(self) -> None:
        """AsyncWorkerPool.stop() uses WorkerConfig.stop_timeout as default."""
        import asyncio
        from unittest.mock import patch

        pool = AsyncWorkerPool(config=WorkerConfig(worker_count=2, stop_timeout=8.0))
        pool.start()

        # Add a task that outlasts our wait so asyncio.wait is actually called
        async def slow() -> None:
            await asyncio.sleep(10)

        task = asyncio.create_task(slow())
        pool._tasks.add(task)
        task.add_done_callback(pool._tasks.discard)

        wait_timeouts: list[float | None] = []
        original_wait = asyncio.wait

        async def recording_wait(
            fs: object, timeout: float | None = None, **kw: object
        ) -> object:
            wait_timeouts.append(timeout)
            return await original_wait(fs, timeout=0.01, **kw)  # type: ignore[arg-type]

        with patch("rabbitkit.concurrency.asyncio.wait", side_effect=recording_wait):
            await pool.stop()

        task.cancel()
        assert wait_timeouts == [8.0]


class TestSyncWorkerPoolTimeout:
    def test_stop_warns_on_timed_out_tasks(self) -> None:
        """Line 73: warning logged when tasks don't complete within stop timeout."""
        import time
        from unittest.mock import patch

        from rabbitkit.concurrency import SyncWorkerPool
        from rabbitkit.core.config import WorkerConfig

        pool = SyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()

        started = threading.Event()

        def slow_task() -> None:
            started.set()
            time.sleep(5)  # longer than stop timeout

        future = pool._executor.submit(slow_task)  # type: ignore[union-attr]
        with pool._futures_lock:
            pool._futures.add(future)

        started.wait(timeout=1.0)

        with patch("rabbitkit.concurrency.logger") as mock_logger:
            pool.stop(timeout=0.05)  # very short timeout

        mock_logger.warning.assert_called_once()
        assert "did not complete" in mock_logger.warning.call_args[0][0]
        future.cancel()

    def test_pending_count_when_executor_is_none(self) -> None:
        """Line 108: pending_count returns 0 when pool not started."""
        from rabbitkit.concurrency import SyncWorkerPool
        from rabbitkit.core.config import WorkerConfig

        pool = SyncWorkerPool(WorkerConfig(worker_count=2))
        # Don't call start() — _executor stays None
        assert pool.pending_count == 0
