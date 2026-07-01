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


class _FakeRedisForLock:
    """In-memory Redis mock that implements SET NX EX and an EVAL that mimics
    the compare-and-delete release script."""

    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self.eval_calls: list[tuple] = []

    def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool | None:
        if nx and key in self._store:
            return None
        self._store[key] = value
        return True

    def get(self, key: str) -> str | None:
        return self._store.get(key)

    def delete(self, key: str) -> int:
        if key in self._store:
            del self._store[key]
            return 1
        return 0

    def eval(self, script: str, numkeys: int, key: str, arg: str) -> int:
        self.eval_calls.append((script, numkeys, key, arg))
        stored = self._store.get(key)
        if stored is not None and stored == arg:
            del self._store[key]
            return 1
        return 0


class _FakeAsyncRedisForLock:
    """Async in-memory Redis mock mirroring _FakeRedisForLock."""

    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self.eval_calls: list[tuple] = []

    async def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool | None:
        if nx and key in self._store:
            return None
        self._store[key] = value
        return True

    async def get(self, key: str) -> str | None:
        return self._store.get(key)

    async def delete(self, key: str) -> int:
        if key in self._store:
            del self._store[key]
            return 1
        return 0

    async def eval(self, script: str, numkeys: int, key: str, arg: str) -> int:
        self.eval_calls.append((script, numkeys, key, arg))
        stored = self._store.get(key)
        if stored is not None and stored == arg:
            del self._store[key]
            return 1
        return 0


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
        """Release atomically deletes (via Lua eval) when the stored value matches."""
        redis = _FakeRedisForLock()
        lock = RedisLock(redis, prefix="test:", ttl=60)

        assert lock.acquire("k") is True
        lock_value = lock.fencing_token("k")
        assert lock_value is not None

        lock.release("k")

        assert len(redis.eval_calls) == 1
        _script, _numkeys, key, arg = redis.eval_calls[0]
        assert key == "test:k"
        assert arg == lock_value
        assert "k" not in redis._store  # actually deleted
        assert lock.fencing_token("k") is None

    def test_release_does_not_delete_when_value_differs(self) -> None:
        """A stale holder must NOT delete a lock owned by someone else (H-S2)."""
        redis = _FakeRedisForLock()
        lock_a = RedisLock(redis, prefix="test:", ttl=60)

        # Holder A acquires.
        assert lock_a.acquire("k") is True
        value_a = lock_a.fencing_token("k")
        assert value_a is not None

        # Simulate A's lock expiring and B re-acquiring with a new UUID.
        del redis._store["test:k"]
        lock_b = RedisLock(redis, prefix="test:", ttl=60)
        assert lock_b.acquire("k") is True
        value_b = lock_b.fencing_token("k")
        assert value_b is not None and value_b != value_a

        # A's stale release must NOT delete B's lock.
        # Re-inject A's value into the lock's tracking and release via A.
        lock_a._lock_values["k"] = value_a  # type: ignore[index]
        lock_a.release("k")

        assert len(redis.eval_calls) == 1
        assert redis._store.get("test:k") == value_b  # B's lock survives

    def test_release_without_acquire_is_noop(self) -> None:
        redis = _FakeRedisForLock()
        lock = RedisLock(redis)
        lock.release("never-acquired")
        assert redis.eval_calls == []

    def test_release_eval_failure_keeps_lock_tracked_and_warns(self, caplog: pytest.LogCaptureFixture) -> None:
        # L-5: a Redis EVAL transport error must NOT clear local tracking and
        # must be logged (not silently swallowed).
        import logging

        class _ExplodingRedis(_FakeRedisForLock):
            def eval(self, script, numkeys, key, arg):
                raise ConnectionError("redis down")

        redis = _ExplodingRedis()
        lock = RedisLock(redis, prefix="test:", ttl=60)
        assert lock.acquire("k") is True
        lock_value = lock.fencing_token("k")
        assert lock_value is not None

        with caplog.at_level(logging.WARNING, logger="rabbitkit.locking"):
            lock.release("k")

        # Local tracking preserved so the TTL / a later retry can clean up.
        assert lock.fencing_token("k") == lock_value
        assert any("Redis EVAL failed" in rec.message for rec in caplog.records)

    def test_fencing_token_returns_uuid_after_acquire(self) -> None:
        redis = _FakeRedisForLock()
        lock = RedisLock(redis)
        assert lock.fencing_token("k") is None
        assert lock.acquire("k") is True
        token = lock.fencing_token("k")
        assert isinstance(token, str) and len(token) == 32
        lock.release("k")
        assert lock.fencing_token("k") is None

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
        redis = _FakeAsyncRedisForLock()
        lock = RedisLock(redis)

        await lock.acquire_async("k")
        lock_value = lock.fencing_token("k")
        assert lock_value is not None

        await lock.release_async("k")
        assert len(redis.eval_calls) == 1
        _script, _numkeys, key, arg = redis.eval_calls[0]
        assert key.endswith("k")
        assert arg == lock_value
        assert "k" not in redis._store

    @pytest.mark.asyncio
    async def test_release_async_skips_when_value_differs(self) -> None:
        redis = _FakeAsyncRedisForLock()
        lock_a = RedisLock(redis)
        await lock_a.acquire_async("k")
        value_a = lock_a.fencing_token("k")
        assert value_a is not None

        # Expire A, re-acquire as B.
        del redis._store["rabbitkit:lock:k"]
        lock_b = RedisLock(redis)
        await lock_b.acquire_async("k")
        value_b = lock_b.fencing_token("k")
        assert value_b is not None and value_b != value_a

        lock_a._lock_values["k"] = value_a  # type: ignore[index]
        await lock_a.release_async("k")
        assert redis._store.get("rabbitkit:lock:k") == value_b

    @pytest.mark.asyncio
    async def test_release_async_without_acquire_is_noop(self) -> None:
        redis = _FakeAsyncRedisForLock()
        lock = RedisLock(redis)
        await lock.release_async("never-acquired")
        assert redis.eval_calls == []

    @pytest.mark.asyncio
    async def test_release_async_eval_failure_keeps_lock_tracked_and_warns(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        # L-5: async EVAL transport error must NOT clear local tracking and must
        # be logged (not silently swallowed).
        import logging

        class _ExplodingAsyncRedis(_FakeAsyncRedisForLock):
            async def eval(self, script, numkeys, key, arg):
                raise ConnectionError("redis down")

        redis = _ExplodingAsyncRedis()
        lock = RedisLock(redis, prefix="test:", ttl=60)
        assert await lock.acquire_async("k") is True
        lock_value = lock.fencing_token("k")
        assert lock_value is not None

        with caplog.at_level(logging.WARNING, logger="rabbitkit.locking"):
            await lock.release_async("k")

        assert lock.fencing_token("k") == lock_value
        assert any("Redis EVAL failed" in rec.message for rec in caplog.records)


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


# ── AttributeError in _eval_release / _eval_release_async ─────────────────


class TestRedisLockEvalAttributeError:
    """Line 178: _eval_release returns False when redis client has no ``eval``.
    Line 219: _eval_release_async returns False in the same case.
    """

    def test_eval_release_returns_false_when_no_eval_method(self) -> None:
        """Line 178: ``except AttributeError: return False`` fires when the
        redis client has no ``eval`` attribute at all."""

        class _NoEvalRedis:
            """Redis double that has no ``eval`` method — simulates a client
            that only supports a subset of commands."""

            def __init__(self) -> None:
                self._store: dict[str, str] = {}

            def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool:
                if nx and key in self._store:
                    return False
                self._store[key] = value
                return True

        redis = _NoEvalRedis()
        lock = RedisLock(redis, prefix="test:", ttl=60)

        # Acquire so _lock_values has the key.
        assert lock.acquire("k") is True
        lock_value = lock.fencing_token("k")
        assert lock_value is not None

        # _eval_release will call redis.eval which doesn't exist → AttributeError
        # → returns False.  The local tracking should stay intact (L-5).
        deleted = lock._eval_release("test:k", lock_value)
        assert deleted is False

        # Local tracking preserved because the delete was "not confirmed".
        assert lock.fencing_token("k") == lock_value

    @pytest.mark.asyncio
    async def test_eval_release_async_returns_false_when_no_eval_method(self) -> None:
        """Line 219: ``except AttributeError: return False`` fires in the async
        variant when the redis client has no ``eval`` attribute."""

        class _NoEvalAsyncRedis:
            """Async redis double with no ``eval`` method."""

            def __init__(self) -> None:
                self._store: dict[str, str] = {}

            async def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool:
                if nx and key in self._store:
                    return False
                self._store[key] = value
                return True

        redis = _NoEvalAsyncRedis()
        lock = RedisLock(redis, prefix="test:", ttl=60)

        await lock.acquire_async("k")
        lock_value = lock.fencing_token("k")
        assert lock_value is not None

        deleted = await lock._eval_release_async("test:k", lock_value)
        assert deleted is False

        # Local tracking preserved.
        assert lock.fencing_token("k") == lock_value
