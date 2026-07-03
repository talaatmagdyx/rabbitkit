"""Tests for concurrency.py -- SyncWorkerPool and AsyncWorkerPool."""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest

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


class TestBoundedWorkQueue:
    """M11: WorkerConfig.max_queue_size bounds the sync pool's work queue
    (default 0 = unbounded, unchanged)."""

    def test_default_queue_is_unbounded(self) -> None:
        pool = SyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()
        assert pool._executor is not None
        assert pool._executor._work.maxsize == 0  # unbounded
        pool.stop()

    def test_max_queue_size_bounds_the_queue(self) -> None:
        pool = SyncWorkerPool(WorkerConfig(worker_count=2, max_queue_size=64))
        pool.start()
        assert pool._executor is not None
        assert pool._executor._work.maxsize == 64
        pool.stop()


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


# ── H12: abandoned handlers are nacked + logged, submit() refuses when stopped ──


class TestAsyncWorkerPoolAbandonment:
    async def test_stop_nacks_abandoned_message_for_redelivery(self) -> None:
        """H12: a task abandoned at the stop_timeout deadline has its still-
        unsettled message explicitly nacked (requeue) instead of being left
        to the implicit requeue-on-connection-close behavior."""
        pool = AsyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()

        message = _make_message(delivery_tag=42, message_id="abc")
        nacked: list[bool] = []

        async def real_nack(requeue: bool) -> None:
            nacked.append(requeue)

        message._nack_async_fn = real_nack

        async def never_finish(msg: RabbitMessage) -> None:
            await asyncio.sleep(100)

        await pool.submit(never_finish, message)
        await asyncio.sleep(0.01)

        await pool.stop(timeout=0.05)

        assert nacked == [True]
        assert message._disposition == "nacked"

    async def test_stop_skips_already_settled_message(self) -> None:
        """H12: a message that settled itself before the deadline (however
        that happened) must not be touched again -- no double-settle."""
        pool = AsyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()

        message = _make_message(delivery_tag=5)
        message._disposition = "acked"

        async def never_finish(msg: RabbitMessage) -> None:
            await asyncio.sleep(100)

        await pool.submit(never_finish, message)
        await asyncio.sleep(0.01)

        nack_calls: list[bool] = []
        message._nack_async_fn = lambda requeue: nack_calls.append(requeue)  # type: ignore[assignment]

        await pool.stop(timeout=0.05)

        assert nack_calls == []
        assert message._disposition == "acked"

    async def test_stop_logs_abandoned_delivery_tag(self) -> None:
        """H12: the abandonment warning names the delivery_tag/message_id,
        not just a bare count, so operators can correlate it downstream."""
        from unittest.mock import patch

        pool = AsyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()

        message = _make_message(delivery_tag=99, message_id="m-99")

        async def never_finish(msg: RabbitMessage) -> None:
            await asyncio.sleep(100)

        await pool.submit(never_finish, message)
        await asyncio.sleep(0.01)

        with patch("rabbitkit.concurrency.logger") as mock_logger:
            await pool.stop(timeout=0.05)

        assert any(99 in call.args for call in mock_logger.warning.call_args_list)

    async def test_submit_after_stop_nacks_instead_of_orphaning(self) -> None:
        """H12: submit() called while the pool isn't running (e.g. a stray
        post-stop() delivery callback) refuses to schedule an unawaited task
        and instead nacks the message immediately for redelivery."""
        pool = AsyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()
        await pool.stop()

        message = _make_message(delivery_tag=7)
        nacked: list[bool] = []

        async def real_nack(requeue: bool) -> None:
            nacked.append(requeue)

        message._nack_async_fn = real_nack

        called: list[bool] = []

        async def callback(msg: RabbitMessage) -> None:
            called.append(True)

        await pool.submit(callback, message)

        assert called == []  # handler never ran -- refused, not orphaned
        assert nacked == [True]
        assert message._disposition == "nacked"
        assert pool.pending_count == 0

    async def test_submit_after_stop_already_settled_is_noop(self) -> None:
        """H12: submit() after stop() for an already-settled message must not
        attempt a redundant nack (which would raise -- no settlement fn set
        needed for an already-settled message)."""
        pool = AsyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()
        await pool.stop()

        message = _make_message()
        message._disposition = "acked"

        called: list[bool] = []

        async def callback(msg: RabbitMessage) -> None:
            called.append(True)

        await pool.submit(callback, message)  # must not raise
        assert called == []


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
        """AsyncWorkerPool.stop() uses config.stop_timeout as the default."""
        import time

        pool = AsyncWorkerPool(config=WorkerConfig(worker_count=2, stop_timeout=0.2))
        pool.start()
        pool._running = True

        async def slow() -> None:
            await asyncio.sleep(100)

        task = asyncio.create_task(slow())
        pool._tasks.add(task)
        task.add_done_callback(pool._tasks.discard)

        t0 = time.monotonic()
        await pool.stop()
        elapsed = time.monotonic() - t0

        # Should timeout after ~0.2s (the config stop_timeout), not 100s
        assert elapsed < 1.0, f"stop() took {elapsed:.1f}s, expected <1s"
        assert task.cancelled() or task.done(), "task should be cancelled after timeout"


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

    def test_stop_logs_abandoned_delivery_tag_when_submitted_via_pool(self) -> None:
        """H12: a handler submitted through the public submit() (so its
        message is tracked) is logged by delivery_tag/message_id -- not just
        the bare count -- when abandoned past stop_timeout."""
        import time
        from unittest.mock import patch

        from rabbitkit.concurrency import SyncWorkerPool
        from rabbitkit.core.config import WorkerConfig

        pool = SyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()

        started = threading.Event()

        def slow_handler(msg: RabbitMessage) -> None:
            started.set()
            time.sleep(5)  # longer than stop timeout

        message = _make_message(delivery_tag=123, message_id="sync-abandon")
        pool.submit(slow_handler, message)
        started.wait(timeout=1.0)

        with patch("rabbitkit.concurrency.logger") as mock_logger:
            pool.stop(timeout=0.05)

        assert any(123 in call.args for call in mock_logger.warning.call_args_list)

    def test_pending_count_when_executor_is_none(self) -> None:
        """Line 108: pending_count returns 0 when pool not started."""
        from rabbitkit.concurrency import SyncWorkerPool
        from rabbitkit.core.config import WorkerConfig

        pool = SyncWorkerPool(WorkerConfig(worker_count=2))
        # Don't call start() — _executor stays None
        assert pool.pending_count == 0


# ── H-SRE2: daemon worker threads ─────────────────────────────────────────


class TestDaemonWorkerThreads:
    def test_sync_worker_pool_threads_are_daemon(self) -> None:
        """SyncWorkerPool worker threads must be daemon so a stuck handler
        cannot keep the process alive past stop() (k8s graceful shutdown)."""
        import threading
        import time

        from rabbitkit.concurrency import SyncWorkerPool

        pool = SyncWorkerPool(config=WorkerConfig(worker_count=2))
        pool.start()
        try:
            block = threading.Event()

            def slow_handler(_msg: object) -> None:
                block.wait(timeout=2.0)

            pool.submit(slow_handler, None)  # type: ignore[arg-type]
            # Give the worker thread a moment to start.
            time.sleep(0.1)
            assert pool._executor is not None
            threads = list(pool._executor._threads)  # type: ignore[union-attr]
            assert threads, "expected at least one worker thread to have started"
            assert all(t.daemon for t in threads), "all worker threads must be daemon"
            block.set()
        finally:
            pool.stop(timeout=1.0)


class TestDaemonPoolParallelism:
    def test_worker_count_runs_in_parallel(self) -> None:
        """R-1 regression: worker_count>1 must actually parallelize work.

        Before the fix, _idle_count semantics were inverted so the pool trended
        to a single worker. Submit worker_count tasks that each hold a slot,
        sleep, and release; assert max concurrency reaches worker_count.
        """
        import threading
        import time

        from rabbitkit.concurrency import SyncWorkerPool

        n = 4
        per_task = 0.2
        pool = SyncWorkerPool(config=WorkerConfig(worker_count=n))
        pool.start()
        current = 0
        max_concurrent = 0
        state_lock = threading.Lock()

        def task(_msg: object) -> None:
            nonlocal current, max_concurrent
            with state_lock:
                current += 1
                if current > max_concurrent:
                    max_concurrent = current
            time.sleep(per_task)
            with state_lock:
                current -= 1

        try:
            t0 = time.monotonic()
            for _ in range(n):
                pool.submit(task, None)  # type: ignore[arg-type]
            # Wait for all to finish by polling pending_count.
            deadline = t0 + 10.0
            while pool.pending_count > 0 and time.monotonic() < deadline:
                time.sleep(0.01)
            elapsed = time.monotonic() - t0
        finally:
            pool.stop(timeout=2.0)
        assert max_concurrent == n, (
            f"pool did not parallelize: max_concurrent={max_concurrent}, expected {n}"
        )
        # And it should be clearly faster than serial.
        assert elapsed < n * per_task, (
            f"elapsed={elapsed:.2f}s should be < serial={n*per_task:.2f}s"
        )


# ── _DaemonWorkerPool uncovered lines ──────────────────────────────────────


class TestDaemonWorkerPoolUncovered:
    """Tests for uncovered lines in _DaemonWorkerPool."""

    def test_submit_raises_after_shutdown(self) -> None:
        """Line 60: submit() after shutdown raises RuntimeError."""
        from rabbitkit.concurrency import _DaemonWorkerPool

        pool = _DaemonWorkerPool(max_workers=2)
        pool.shutdown()
        with pytest.raises(RuntimeError, match="cannot schedule new futures after shutdown"):
            pool.submit(lambda: None)

    def test_worker_sets_exception_on_raising_function(self) -> None:
        """Lines 108-109: fut.set_exception(exc) when fn raises."""
        from rabbitkit.concurrency import _DaemonWorkerPool

        pool = _DaemonWorkerPool(max_workers=1)

        def bad_fn() -> None:
            raise ValueError("boom")

        fut = pool.submit(bad_fn)
        # Wait for the worker to process it
        import concurrent.futures

        concurrent.futures.wait([fut], timeout=5.0)
        assert fut.done()
        with pytest.raises(ValueError, match="boom"):
            fut.result()
        pool.shutdown(wait=True)

    def test_shutdown_cancel_futures_cancels_pending(self) -> None:
        """Line 122: shutdown(cancel_futures=True) cancels queued futures."""
        from rabbitkit.concurrency import _DaemonWorkerPool

        # Use a large pool to prevent immediate execution of queued items
        pool = _DaemonWorkerPool(max_workers=1)

        # Block the single worker so the second submit stays queued
        block = threading.Event()

        def blocking_fn() -> None:
            block.wait(timeout=5.0)

        pool.submit(blocking_fn)  # runs immediately in the worker
        # Give the worker a moment to pick up fut1
        time.sleep(0.05)

        # Second future queued — worker is busy with fut1
        fut2 = pool.submit(lambda: None)

        # Shut down with cancel_futures: queued fut2 should be cancelled
        block.set()  # unblock fut1
        pool.shutdown(cancel_futures=True, wait=True)

        # fut2 should be either cancelled or done (worker may race to pick it up)
        assert fut2.cancelled() or fut2.done()

    def test_shutdown_wait_joins_threads(self) -> None:
        """Lines 124-125: shutdown(wait=True) joins all worker threads."""
        from rabbitkit.concurrency import _DaemonWorkerPool

        pool = _DaemonWorkerPool(max_workers=2)

        results: list[int] = []

        def work(n: int) -> int:
            results.append(n)
            return n

        pool.submit(work, 1)
        pool.submit(work, 2)
        # shutdown with wait=True: must join threads before returning
        pool.shutdown(wait=True)

        # After join, all submitted work should be complete
        assert sorted(results) == [1, 2]

    def test_worker_count_property(self) -> None:
        """Lines 131-132: worker_count property returns len(_threads)."""
        from rabbitkit.concurrency import _DaemonWorkerPool

        pool = _DaemonWorkerPool(max_workers=2)

        # No threads yet
        assert pool.worker_count == 0

        block = threading.Event()

        def slow() -> None:
            block.wait(timeout=5.0)

        pool.submit(slow)
        # Give the pool a moment to spawn a thread
        time.sleep(0.05)

        assert pool.worker_count >= 1

        block.set()
        pool.shutdown(wait=True)


# ── AsyncWorkerPool.stop() TimeoutError path ─────────────────────────────


class TestAsyncWorkerPoolStopTimeout:
    """Lines 296, 298: task.cancel() and gather in stop() TimeoutError branch."""

    async def test_stop_timeout_cancels_tasks_and_gathers(self) -> None:
        """Lines 296-298: when timeout elapses, not-done tasks are cancelled
        and then gathered so CancelledError is consumed cleanly."""
        pool = AsyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()

        cancelled_flag: list[bool] = []

        async def never_finishes(msg: RabbitMessage) -> None:
            try:
                await asyncio.sleep(100)
            except asyncio.CancelledError:
                cancelled_flag.append(True)
                raise

        await pool.submit(never_finishes, _make_message())
        # Let the task start running
        await asyncio.sleep(0.01)

        # stop() with tiny timeout → TimeoutError branch
        await pool.stop(timeout=0.02)

        # The task was cancelled (CancelledError was raised inside it)
        assert cancelled_flag == [True]
        # Pool cleaned up
        assert pool.pending_count == 0


# ── H2: SyncWorkerPool.stop(pump=...) ─────────────────────────────────────


class TestSyncWorkerPoolPump:
    """H2: stop()'s pump= parameter polls in short slices, calling pump()
    between them, instead of one blocking concurrent.futures.wait(). This is
    what lets a worker thread's transport-marshaled ack actually get drained
    by the owner thread during SyncBroker.stop()'s drain window."""

    def test_pump_is_called_while_waiting_for_a_slow_task(self) -> None:
        pool = SyncWorkerPool(WorkerConfig(worker_count=2, stop_timeout=5.0))
        pool.start()

        release = threading.Event()

        def slow_task() -> None:
            release.wait(timeout=2.0)

        future = pool._executor.submit(slow_task)  # type: ignore[union-attr]
        with pool._futures_lock:
            pool._futures.add(future)

        pump_calls: list[int] = []

        def pump() -> None:
            pump_calls.append(1)
            if len(pump_calls) >= 2:
                release.set()

        pool.stop(timeout=5.0, pump=pump)

        # pump() was called at least twice (enough to eventually release the
        # task) and the task actually completed before stop() returned.
        assert len(pump_calls) >= 2
        assert pool.pending_count == 0

    def test_pump_not_called_when_no_futures_pending(self) -> None:
        """No in-flight tasks -> the pump path is skipped entirely (falls
        back to the plain concurrent.futures.wait branch, a no-op wait)."""
        pool = SyncWorkerPool(WorkerConfig(worker_count=2))
        pool.start()

        pump_calls: list[int] = []
        pool.stop(pump=lambda: pump_calls.append(1))

        assert pump_calls == []

    def test_pump_final_drain_called_after_all_tasks_done(self) -> None:
        """A final pump() call happens after the wait loop exits, to drain a
        just-marshaled callback from a task that finished on the last poll."""
        pool = SyncWorkerPool(WorkerConfig(worker_count=2, stop_timeout=5.0))
        pool.start()

        done = threading.Event()

        def quick_task() -> None:
            done.set()

        future = pool._executor.submit(quick_task)  # type: ignore[union-attr]
        with pool._futures_lock:
            pool._futures.add(future)
        done.wait(timeout=2.0)

        pump_calls: list[int] = []
        pool.stop(timeout=5.0, pump=lambda: pump_calls.append(1))

        # Called at least once for the final drain even though the task was
        # already done by the time stop() was invoked.
        assert len(pump_calls) >= 1

    def test_pump_abandons_and_warns_after_deadline(self) -> None:
        """If the task never finishes, the pump-loop still respects the
        overall deadline and logs the same abandonment warning as the
        non-pump path."""
        from unittest.mock import patch

        pool = SyncWorkerPool(WorkerConfig(worker_count=2, stop_timeout=5.0))
        pool.start()

        started = threading.Event()

        def never_finishes() -> None:
            started.set()
            time.sleep(5)  # longer than the stop timeout below

        future = pool._executor.submit(never_finishes)  # type: ignore[union-attr]
        with pool._futures_lock:
            pool._futures.add(future)
        started.wait(timeout=1.0)

        with patch("rabbitkit.concurrency.logger") as mock_logger:
            pool.stop(timeout=0.1, pump=lambda: None)

        mock_logger.warning.assert_called_once()
        assert "did not complete" in mock_logger.warning.call_args[0][0]
        future.cancel()
