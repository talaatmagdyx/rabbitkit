"""Tests for locking.py — DistributedLock, RedisLock, LockMiddleware."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.locking import DistributedLock, LockMiddleware, RedisLock

# ── helpers ───────────────────────────────────────────────────────────────


def _make_message(routing_key: str = "test.key") -> RabbitMessage:
    msg = RabbitMessage(body=b"hello", routing_key=routing_key)
    msg._nack_fn = MagicMock()
    msg._ack_fn = MagicMock()
    msg._nack_async_fn = AsyncMock()
    msg._ack_async_fn = AsyncMock()
    return msg


# ── RedisLock tests ──────────────────────────────────────────────────────


class TestRedisLock:
    def test_acquire_success(self) -> None:
        redis = MagicMock()
        redis.set.return_value = True
        lock = RedisLock(redis, prefix="test:", ttl=60)

        assert lock.acquire("my-key") is True
        redis.set.assert_called_once()
        call_kwargs = redis.set.call_args
        assert call_kwargs[1]["nx"] is True
        assert call_kwargs[1]["ex"] == 60

    def test_acquire_failure(self) -> None:
        redis = MagicMock()
        redis.set.return_value = False
        lock = RedisLock(redis)

        assert lock.acquire("my-key") is False

    def test_release_deletes_when_value_matches(self) -> None:
        redis = MagicMock()
        redis.set.return_value = True
        lock = RedisLock(redis)

        lock.acquire("k")
        lock_value = lock._lock_values["k"]
        redis.get.return_value = lock_value

        lock.release("k")
        redis.delete.assert_called_once()
        assert "k" not in lock._lock_values

    def test_release_deletes_when_value_matches_bytes(self) -> None:
        redis = MagicMock()
        redis.set.return_value = True
        lock = RedisLock(redis)

        lock.acquire("k")
        lock_value = lock._lock_values["k"]
        redis.get.return_value = lock_value.encode()

        lock.release("k")
        redis.delete.assert_called_once()

    def test_release_skips_delete_when_value_differs(self) -> None:
        redis = MagicMock()
        redis.set.return_value = True
        lock = RedisLock(redis)

        lock.acquire("k")
        redis.get.return_value = "someone-elses-value"

        lock.release("k")
        redis.delete.assert_not_called()

    def test_release_without_acquire_is_noop(self) -> None:
        redis = MagicMock()
        lock = RedisLock(redis)
        lock.release("never-acquired")
        redis.get.assert_not_called()
        redis.delete.assert_not_called()

    def test_key_prefix(self) -> None:
        redis = MagicMock()
        lock = RedisLock(redis, prefix="myapp:")
        assert lock._key("foo") == "myapp:foo"

    @pytest.mark.asyncio
    async def test_acquire_async_success(self) -> None:
        redis = AsyncMock()
        redis.set.return_value = True
        lock = RedisLock(redis)

        assert await lock.acquire_async("k") is True

    @pytest.mark.asyncio
    async def test_acquire_async_failure(self) -> None:
        redis = AsyncMock()
        redis.set.return_value = False
        lock = RedisLock(redis)

        assert await lock.acquire_async("k") is False

    @pytest.mark.asyncio
    async def test_release_async_deletes_when_value_matches(self) -> None:
        redis = AsyncMock()
        redis.set.return_value = True
        lock = RedisLock(redis)

        await lock.acquire_async("k")
        lock_value = lock._lock_values["k"]
        redis.get.return_value = lock_value

        await lock.release_async("k")
        redis.delete.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_release_async_skips_when_value_differs(self) -> None:
        redis = AsyncMock()
        redis.set.return_value = True
        lock = RedisLock(redis)

        await lock.acquire_async("k")
        redis.get.return_value = "other-value"

        await lock.release_async("k")
        redis.delete.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_release_async_without_acquire_is_noop(self) -> None:
        redis = AsyncMock()
        lock = RedisLock(redis)
        await lock.release_async("never-acquired")
        redis.get.assert_not_awaited()


# ── LockMiddleware tests ─────────────────────────────────────────────────


class TestLockMiddleware:
    def test_acquire_success_calls_handler(self) -> None:
        lock = MagicMock(spec=DistributedLock)
        lock.acquire.return_value = True
        mw = LockMiddleware(lock)

        msg = _make_message()
        handler = MagicMock(return_value="result")

        result = mw.consume_scope(handler, msg)

        assert result == "result"
        handler.assert_called_once_with(msg)
        lock.acquire.assert_called_once_with("test.key", 0.0)  # non-blocking default
        lock.release.assert_called_once_with("test.key")

    def test_acquire_fail_nacks_with_requeue(self) -> None:
        lock = MagicMock(spec=DistributedLock)
        lock.acquire.return_value = False
        mw = LockMiddleware(lock)

        msg = _make_message()
        handler = MagicMock()

        result = mw.consume_scope(handler, msg)

        assert result is None
        handler.assert_not_called()
        msg._nack_fn.assert_called_once_with(True)

    def test_acquire_fail_skips_nack_if_already_settled(self) -> None:
        lock = MagicMock(spec=DistributedLock)
        lock.acquire.return_value = False
        mw = LockMiddleware(lock)

        msg = _make_message()
        msg._disposition = "acked"  # already settled
        handler = MagicMock()

        result = mw.consume_scope(handler, msg)

        assert result is None
        msg._nack_fn.assert_not_called()

    def test_release_called_on_exception(self) -> None:
        lock = MagicMock(spec=DistributedLock)
        lock.acquire.return_value = True
        mw = LockMiddleware(lock)

        msg = _make_message()
        handler = MagicMock(side_effect=ValueError("boom"))

        with pytest.raises(ValueError, match="boom"):
            mw.consume_scope(handler, msg)

        lock.release.assert_called_once_with("test.key")

    def test_custom_key_fn(self) -> None:
        lock = MagicMock(spec=DistributedLock)
        lock.acquire.return_value = True
        key_fn = lambda m: f"custom:{m.routing_key}"
        mw = LockMiddleware(lock, key_fn=key_fn)

        msg = _make_message(routing_key="orders.created")
        handler = MagicMock(return_value="ok")

        mw.consume_scope(handler, msg)

        lock.acquire.assert_called_once_with("custom:orders.created", 0.0)
        lock.release.assert_called_once_with("custom:orders.created")

    def test_custom_timeout(self) -> None:
        lock = MagicMock(spec=DistributedLock)
        lock.acquire.return_value = True
        mw = LockMiddleware(lock, timeout=5.0)

        msg = _make_message()
        handler = MagicMock()

        mw.consume_scope(handler, msg)

        lock.acquire.assert_called_once_with("test.key", 5.0)

    @pytest.mark.asyncio
    async def test_async_acquire_success(self) -> None:
        lock = AsyncMock(spec=DistributedLock)
        lock.acquire_async.return_value = True
        mw = LockMiddleware(lock)

        msg = _make_message()
        handler = AsyncMock(return_value="async-result")

        result = await mw.consume_scope_async(handler, msg)

        assert result == "async-result"
        handler.assert_awaited_once_with(msg)
        lock.acquire_async.assert_awaited_once_with("test.key", 0.0)
        lock.release_async.assert_awaited_once_with("test.key")

    @pytest.mark.asyncio
    async def test_async_acquire_fail_nacks(self) -> None:
        lock = AsyncMock(spec=DistributedLock)
        lock.acquire_async.return_value = False
        mw = LockMiddleware(lock)

        msg = _make_message()
        handler = AsyncMock()

        result = await mw.consume_scope_async(handler, msg)

        assert result is None
        handler.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_async_release_on_exception(self) -> None:
        lock = AsyncMock(spec=DistributedLock)
        lock.acquire_async.return_value = True
        mw = LockMiddleware(lock)

        msg = _make_message()
        handler = AsyncMock(side_effect=RuntimeError("async boom"))

        with pytest.raises(RuntimeError, match="async boom"):
            await mw.consume_scope_async(handler, msg)

        lock.release_async.assert_awaited_once_with("test.key")


# ── Protocol check ───────────────────────────────────────────────────────


class TestDistributedLockProtocol:
    def test_redis_lock_satisfies_protocol(self) -> None:
        redis = MagicMock()
        lock = RedisLock(redis)
        assert isinstance(lock, DistributedLock)


class TestRedisLockTimeout:
    def test_timeout_zero_is_single_non_blocking_attempt(self) -> None:
        redis = MagicMock()
        redis.set.return_value = False  # always locked
        lock = RedisLock(redis)

        assert lock.acquire("k", timeout=0) is False
        assert redis.set.call_count == 1  # did NOT poll

    def test_positive_timeout_polls_then_gives_up(self) -> None:
        import time

        redis = MagicMock()
        redis.set.return_value = False  # never acquirable
        lock = RedisLock(redis)

        start = time.monotonic()
        assert lock.acquire("k", timeout=0.15) is False
        elapsed = time.monotonic() - start

        assert elapsed >= 0.15  # waited up to the timeout
        assert redis.set.call_count >= 2  # polled more than once

    def test_concurrent_acquire_distinct_keys_is_thread_safe(self) -> None:
        import threading

        redis = MagicMock()
        redis.set.return_value = True  # every distinct key acquires
        lock = RedisLock(redis)

        def worker(i: int) -> None:
            assert lock.acquire(f"key-{i}", timeout=0) is True

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(50)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # _lock_values guarded by a lock — all 50 keys recorded, no lost writes.
        assert len(lock._lock_values) == 50
