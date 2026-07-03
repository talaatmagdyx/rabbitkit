"""Tests for middleware/deduplication.py — DeduplicationMiddleware."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from rabbitkit.core.config import DeduplicationConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import (
    REQUEUED_FOR_RETRY,
    DeduplicationMarkPolicy,
    PublishOutcome,
    PublishStatus,
)
from rabbitkit.middleware.deduplication import DeduplicationMiddleware
from rabbitkit.middleware.retry import RetryMiddleware

# ── helpers ───────────────────────────────────────────────────────────────


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {
        "body": b'{"id": 1}',
        "routing_key": "orders",
        "message_id": "msg-001",
        "correlation_id": "corr-001",
    }
    defaults.update(kwargs)
    return RabbitMessage(**defaults)  # type: ignore[arg-type]


async def _noop_async_ack() -> None:
    """Async no-op settlement fn — RetryMiddleware acks the source message
    when it successfully routes a failure to a delay queue."""


class _FakeRedis:
    """In-memory Redis mock for sync dedup tests."""

    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self.set_calls: list[dict] = []
        self.exists_calls: list[str] = []

    def exists(self, key: str) -> int:
        self.exists_calls.append(key)
        return 1 if key in self._store else 0

    def get(self, key: str) -> str | None:
        return self._store.get(key)

    def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool | None:
        self.set_calls.append({"key": key, "value": value, "nx": nx, "ex": ex})
        if nx and key in self._store:
            return None  # key exists → duplicate
        self._store[key] = value
        return True  # key was set → new

    def clear(self) -> None:
        self._store.clear()
        self.set_calls.clear()
        self.exists_calls.clear()


class _FakeAsyncRedis:
    """In-memory async Redis mock for async dedup tests."""

    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self.set_calls: list[dict] = []
        self.exists_calls: list[str] = []

    async def exists(self, key: str) -> int:
        self.exists_calls.append(key)
        return 1 if key in self._store else 0

    async def get(self, key: str) -> str | None:
        return self._store.get(key)

    async def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool | None:
        self.set_calls.append({"key": key, "value": value, "nx": nx, "ex": ex})
        if nx and key in self._store:
            return None
        self._store[key] = value
        return True


class _ErrorRedis:
    """Redis mock that always raises on exists/set."""

    def exists(self, *args: object, **kwargs: object) -> None:
        raise ConnectionError("Redis is down")

    def set(self, *args: object, **kwargs: object) -> None:
        raise ConnectionError("Redis is down")


class _AsyncErrorRedis:
    """Async Redis mock that always raises on exists/set."""

    async def exists(self, *args: object, **kwargs: object) -> None:
        raise ConnectionError("Redis is down")

    async def set(self, *args: object, **kwargs: object) -> None:
        raise ConnectionError("Redis is down")


# ── new message passes through ───────────────────────────────────────────


class TestNewMessage:
    def test_new_message_passes_through(self) -> None:
        """A new (non-duplicate) message is processed normally."""
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis)

        msg = _make_message()
        handler = MagicMock(return_value="processed")
        result = mw.consume_scope(handler, msg)

        handler.assert_called_once_with(msg)
        assert result == "processed"

    def test_redis_set_called_with_correct_params(self) -> None:
        """Redis SET is called with nx=True and correct TTL."""
        redis = _FakeRedis()
        config = DeduplicationConfig(key_prefix="dedup", ttl=3600)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg = _make_message(message_id="unique-id")
        mw.consume_scope(MagicMock(), msg)

        assert len(redis.set_calls) == 1
        call = redis.set_calls[0]
        assert call["key"] == "dedup:unique-id"
        assert call["nx"] is True
        assert call["ex"] == 3600


# ── duplicate message acked and skipped ──────────────────────────────────


class TestDuplicate:
    def test_duplicate_acked_and_skipped(self) -> None:
        """A duplicate message is acked and the handler is NOT called."""
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis)

        msg = _make_message(message_id="dup-id")
        ack_fn = MagicMock()
        msg._ack_fn = ack_fn

        handler = MagicMock(return_value="processed")

        # First call — new
        result1 = mw.consume_scope(handler, msg)
        assert result1 == "processed"
        handler.assert_called_once()

        # Reset for second message
        handler.reset_mock()
        msg2 = _make_message(message_id="dup-id")
        msg2._ack_fn = MagicMock()

        # Second call — duplicate
        result2 = mw.consume_scope(handler, msg2)
        assert result2 is None
        handler.assert_not_called()
        msg2._ack_fn.assert_called_once()

    def test_already_settled_not_double_acked(self) -> None:
        """If message is already settled, duplicate detection does not re-ack."""
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis)

        # First call seeds the key
        msg1 = _make_message(message_id="settled-id")
        msg1._ack_fn = MagicMock()
        mw.consume_scope(MagicMock(), msg1)

        # Second message — already settled
        msg2 = _make_message(message_id="settled-id")
        msg2._ack_fn = MagicMock()
        msg2._disposition = "acked"  # already settled

        mw.consume_scope(MagicMock(), msg2)

        # Should NOT have called ack again
        msg2._ack_fn.assert_not_called()


# ── key_source variants ──────────────────────────────────────────────────


class TestKeySource:
    def test_key_source_message_id(self) -> None:
        """key_source='message_id' uses message.message_id."""
        redis = _FakeRedis()
        config = DeduplicationConfig(key_source="message_id", key_prefix="test")
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg = _make_message(message_id="mid-123")
        mw.consume_scope(MagicMock(), msg)

        assert redis.set_calls[0]["key"] == "test:mid-123"

    def test_key_source_correlation_id(self) -> None:
        """key_source='correlation_id' uses message.correlation_id."""
        redis = _FakeRedis()
        config = DeduplicationConfig(key_source="correlation_id", key_prefix="test")
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg = _make_message(correlation_id="corr-456")
        mw.consume_scope(MagicMock(), msg)

        assert redis.set_calls[0]["key"] == "test:corr-456"

    def test_key_source_body_hash(self) -> None:
        """key_source='body_hash' uses SHA-256 of message body."""
        import hashlib

        redis = _FakeRedis()
        config = DeduplicationConfig(key_source="body_hash", key_prefix="hash")
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        body = b'{"order": "abc"}'
        msg = _make_message(body=body)
        mw.consume_scope(MagicMock(), msg)

        expected_hash = hashlib.sha256(body).hexdigest()
        assert redis.set_calls[0]["key"] == f"hash:{expected_hash}"

    def test_unknown_key_source_falls_back_to_message_id(self) -> None:
        """Unknown key_source falls back to message_id."""
        redis = _FakeRedis()
        config = DeduplicationConfig(key_source="unknown_field", key_prefix="fb")
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg = _make_message(message_id="fallback-id")
        mw.consume_scope(MagicMock(), msg)

        assert redis.set_calls[0]["key"] == "fb:fallback-id"

    def test_none_message_id_falls_back_to_body_hash(self) -> None:
        """If message_id is None, key falls back to the body hash (not empty)."""
        import hashlib

        redis = _FakeRedis()
        config = DeduplicationConfig(key_source="message_id", key_prefix="test")
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        body = b'{"id": 1}'
        msg = _make_message(body=body, message_id=None)
        mw.consume_scope(MagicMock(), msg)

        expected_hash = hashlib.sha256(body).hexdigest()
        assert redis.set_calls[0]["key"] == f"test:{expected_hash}"

    def test_none_correlation_id_falls_back_to_body_hash(self) -> None:
        """If correlation_id is None, key falls back to the body hash."""
        import hashlib

        redis = _FakeRedis()
        config = DeduplicationConfig(key_source="correlation_id", key_prefix="c")
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        body = b'{"id": 2}'
        msg = _make_message(body=body, correlation_id=None, message_id=None)
        mw.consume_scope(MagicMock(), msg)

        expected_hash = hashlib.sha256(body).hexdigest()
        assert redis.set_calls[0]["key"] == f"c:{expected_hash}"

    def test_distinct_id_less_messages_are_not_deduped(self) -> None:
        """Two id-less messages with different bodies are NOT collapsed (L-S1)."""
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis)

        msg_a = _make_message(body=b'{"order": "a"}', message_id=None)
        msg_b = _make_message(body=b'{"order": "b"}', message_id=None)

        handler = MagicMock(return_value="processed")
        r1 = mw.consume_scope(handler, msg_a)
        r2 = mw.consume_scope(handler, msg_b)

        assert r1 == "processed"
        assert r2 == "processed"  # NOT deduped — different bodies → different keys
        assert handler.call_count == 2
        # Two distinct keys written
        keys = {c["key"] for c in redis.set_calls}
        assert len(keys) == 2


# ── custom key_fn ────────────────────────────────────────────────────────


class TestCustomKeyFn:
    def test_custom_key_fn_overrides_key_source(self) -> None:
        """Custom key_fn takes priority over config.key_source."""
        redis = _FakeRedis()
        config = DeduplicationConfig(key_source="message_id", key_prefix="pfx")
        mw = DeduplicationMiddleware(
            redis_client=redis,
            config=config,
            key_fn=lambda msg: f"custom-{msg.routing_key}",
        )

        msg = _make_message(routing_key="orders.created")
        mw.consume_scope(MagicMock(), msg)

        assert redis.set_calls[0]["key"] == "pfx:custom-orders.created"


# ── key prefix ───────────────────────────────────────────────────────────


class TestKeyPrefix:
    def test_key_prefix(self) -> None:
        """Key prefix is prepended to the extracted key."""
        redis = _FakeRedis()
        config = DeduplicationConfig(key_prefix="myapp:dedup", key_source="message_id")
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg = _make_message(message_id="id-1")
        mw.consume_scope(MagicMock(), msg)

        assert redis.set_calls[0]["key"] == "myapp:dedup:id-1"


# ── TTL ──────────────────────────────────────────────────────────────────


class TestTTL:
    def test_ttl_passed_to_redis(self) -> None:
        """TTL from config is passed to Redis SET."""
        redis = _FakeRedis()
        config = DeduplicationConfig(ttl=7200)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg = _make_message()
        mw.consume_scope(MagicMock(), msg)

        assert redis.set_calls[0]["ex"] == 7200


# ── Redis error fallback ────────────────────────────────────────────────


class TestRedisError:
    def test_fallback_on_redis_error_processes_message(self) -> None:
        """With fallback_on_redis_error=True, message is processed on Redis error."""
        redis = _ErrorRedis()
        config = DeduplicationConfig(fallback_on_redis_error=True)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg = _make_message()
        handler = MagicMock(return_value="fallback-result")
        result = mw.consume_scope(handler, msg)

        handler.assert_called_once_with(msg)
        assert result == "fallback-result"

    def test_fallback_logs_at_error_not_warning(self, caplog: pytest.LogCaptureFixture) -> None:
        """M9: a fallback is an operational event worth alerting on -- logged
        at ERROR, not WARNING."""
        import logging

        redis = _ErrorRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=DeduplicationConfig(fallback_on_redis_error=True))
        msg = _make_message()

        with caplog.at_level(logging.WARNING, logger="rabbitkit.middleware.deduplication"):
            mw.consume_scope(MagicMock(return_value="ok"), msg)

        error_records = [r for r in caplog.records if r.levelno == logging.ERROR]
        assert len(error_records) == 1
        assert "fallback_on_redis_error=False" in error_records[0].message

    def test_fallback_emits_dedup_fallback_total_when_metrics_wired(self) -> None:
        """M9: with a metrics_collector wired in, a fallback increments
        dedup_fallback_total."""
        from rabbitkit.core.config import MetricsConfig

        collector = MagicMock()
        redis = _ErrorRedis()
        mw = DeduplicationMiddleware(
            redis_client=redis,
            config=DeduplicationConfig(fallback_on_redis_error=True),
            metrics_collector=collector,
            metrics_config=MetricsConfig(),
        )
        msg = _make_message(headers={"x-rabbitkit-original-queue": "orders-q"})

        mw.consume_scope(MagicMock(return_value="ok"), msg)

        collector.inc_counter.assert_called_once_with(
            "rabbitkit_dedup_fallback_total", {"queue": "orders-q"}
        )

    def test_fallback_without_metrics_wired_is_noop(self) -> None:
        """No metrics_collector/metrics_config -- must not raise."""
        redis = _ErrorRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=DeduplicationConfig(fallback_on_redis_error=True))
        msg = _make_message()

        mw.consume_scope(MagicMock(return_value="ok"), msg)  # must not raise

    async def test_async_fallback_emits_dedup_fallback_total_when_metrics_wired(self) -> None:
        """M9, async variant."""
        from rabbitkit.core.config import MetricsConfig

        collector = MagicMock()
        redis = _AsyncErrorRedis()
        mw = DeduplicationMiddleware(
            redis_client=redis,
            config=DeduplicationConfig(fallback_on_redis_error=True),
            metrics_collector=collector,
            metrics_config=MetricsConfig(),
        )
        msg = _make_message(headers={"x-rabbitkit-original-queue": "orders-q"})

        async def handler(m: RabbitMessage) -> str:
            return "ok"

        await mw.consume_scope_async(handler, msg)

        collector.inc_counter.assert_called_once_with(
            "rabbitkit_dedup_fallback_total", {"queue": "orders-q"}
        )

    def test_no_fallback_raises_redis_error(self) -> None:
        """With fallback_on_redis_error=False, Redis error is re-raised."""
        redis = _ErrorRedis()
        config = DeduplicationConfig(fallback_on_redis_error=False)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg = _make_message()
        handler = MagicMock()

        with pytest.raises(ConnectionError, match="Redis is down"):
            mw.consume_scope(handler, msg)

        handler.assert_not_called()


# ── async variants ───────────────────────────────────────────────────────


class TestAsync:
    async def test_async_new_message_passes_through(self) -> None:
        """Async: new message is processed normally."""
        redis = _FakeAsyncRedis()
        mw = DeduplicationMiddleware(redis_client=redis)

        msg = _make_message()

        async def handler(m: RabbitMessage) -> str:
            return "async-result"

        result = await mw.consume_scope_async(handler, msg)
        assert result == "async-result"

    async def test_async_duplicate_acked_and_skipped(self) -> None:
        """Async: duplicate message is acked and skipped."""
        redis = _FakeAsyncRedis()
        mw = DeduplicationMiddleware(redis_client=redis)

        # First call — new
        msg1 = _make_message(message_id="async-dup")

        async def handler(m: RabbitMessage) -> str:
            return "processed"

        result1 = await mw.consume_scope_async(handler, msg1)
        assert result1 == "processed"

        # Second call — duplicate
        msg2 = _make_message(message_id="async-dup")
        ack_called = False

        async def async_ack() -> None:
            nonlocal ack_called
            ack_called = True

        msg2._ack_async_fn = async_ack

        call_count = 0

        async def handler2(m: RabbitMessage) -> str:
            nonlocal call_count
            call_count += 1
            return "should not reach"

        result2 = await mw.consume_scope_async(handler2, msg2)
        assert result2 is None
        assert call_count == 0
        assert ack_called is True

    async def test_async_fallback_on_redis_error(self) -> None:
        """Async: processes message on Redis error when fallback=True."""
        redis = _AsyncErrorRedis()
        config = DeduplicationConfig(fallback_on_redis_error=True)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg = _make_message()

        async def handler(m: RabbitMessage) -> str:
            return "fallback-async"

        result = await mw.consume_scope_async(handler, msg)
        assert result == "fallback-async"

    async def test_async_no_fallback_raises(self) -> None:
        """Async: raises Redis error when fallback=False."""
        redis = _AsyncErrorRedis()
        config = DeduplicationConfig(fallback_on_redis_error=False)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg = _make_message()

        async def handler(m: RabbitMessage) -> str:
            return "should not reach"

        with pytest.raises(ConnectionError, match="Redis is down"):
            await mw.consume_scope_async(handler, msg)


# ── default config ───────────────────────────────────────────────────────


class TestDefaultConfig:
    def test_default_config_values(self) -> None:
        """Default config uses expected values."""
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis)

        assert mw._config.key_prefix == "rabbitkit:dedup"
        assert mw._config.ttl == 86400
        assert mw._config.fallback_on_redis_error is True
        assert mw._config.key_source == "message_id"

    def test_custom_config(self) -> None:
        """Custom config overrides defaults."""
        redis = _FakeRedis()
        config = DeduplicationConfig(
            key_prefix="custom",
            ttl=3600,
            fallback_on_redis_error=False,
            key_source="correlation_id",
        )
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        assert mw._config.key_prefix == "custom"
        assert mw._config.ttl == 3600
        assert mw._config.fallback_on_redis_error is False
        assert mw._config.key_source == "correlation_id"


# ── Local LRU cache helpers ──────────────────────────────────────────────


class TestLocalCache:
    def test_local_is_dup_returns_false_when_cache_disabled(self) -> None:
        """_local_is_dup returns False when local_cache_size=0 (disabled)."""
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis)
        # local_cache_size=0 → _local_cache is None
        assert mw._local_cache is None
        assert mw._local_is_dup("some-key") is False

    def test_local_is_dup_returns_false_for_unknown_key(self) -> None:
        """_local_is_dup returns False for a key not yet in the local cache."""
        redis = _FakeRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)
        assert mw._local_is_dup("not-there") is False

    def test_local_is_dup_returns_true_after_mark(self) -> None:
        """_local_is_dup returns True after a key has been marked."""
        redis = _FakeRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)
        mw._local_mark("my-key")
        assert mw._local_is_dup("my-key") is True

    def test_local_mark_noop_when_cache_disabled(self) -> None:
        """_local_mark is a no-op when local_cache_size=0."""
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis)
        # Should not raise; _local_cache is None
        mw._local_mark("key")
        assert mw._local_cache is None

    def test_local_mark_evicts_oldest_when_full(self) -> None:
        """_local_mark evicts the oldest entry when capacity is exceeded."""
        redis = _FakeRedis()
        config = DeduplicationConfig(local_cache_size=2)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        mw._local_mark("key1")
        mw._local_mark("key2")
        # Cache is at capacity; adding key3 should evict key1
        mw._local_mark("key3")

        assert mw._local_cache is not None
        assert len(mw._local_cache) == 2
        assert "key1" not in mw._local_cache
        assert "key2" in mw._local_cache
        assert "key3" in mw._local_cache

    def test_local_remove_noop_when_cache_disabled(self) -> None:
        """_local_remove is a no-op when local_cache_size=0."""
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis)
        # Should not raise; _local_cache is None
        mw._local_remove("key")
        assert mw._local_cache is None

    def test_local_remove_removes_existing_key(self) -> None:
        """_local_remove removes a key from the local cache."""
        redis = _FakeRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)
        mw._local_mark("key-to-remove")
        assert mw._local_is_dup("key-to-remove") is True

        mw._local_remove("key-to-remove")
        assert mw._local_is_dup("key-to-remove") is False

    def test_local_remove_missing_key_is_noop(self) -> None:
        """_local_remove on a key not in the cache does not raise."""
        redis = _FakeRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)
        # Should not raise
        mw._local_remove("nonexistent")


# ── mark_key helpers (sync & async) ─────────────────────────────────────


class TestMarkKey:
    def test_mark_key_returns_true_for_new_key(self) -> None:
        """_mark_key returns True when key is new (Redis SET nx succeeds)."""
        redis = _FakeRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)
        assert mw._mark_key("fresh-key", _make_message()) is True

    def test_mark_key_returns_false_for_local_dup(self) -> None:
        """_mark_key returns False immediately when key is in local cache."""
        redis = _FakeRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)
        mw._local_mark("cached-key")
        assert mw._mark_key("cached-key", _make_message()) is False
        # Redis should NOT have been called
        assert len(redis.set_calls) == 0

    def test_mark_key_returns_false_for_redis_dup(self) -> None:
        """_mark_key returns False when Redis SET nx fails (already exists)."""
        redis = _FakeRedis()
        # Pre-seed the Redis store
        redis._store["taken-key"] = "1"
        mw = DeduplicationMiddleware(redis_client=redis)
        assert mw._mark_key("taken-key", _make_message()) is False

    def test_mark_key_populates_local_cache_on_success(self) -> None:
        """_mark_key records key in local cache after successful Redis SET."""
        redis = _FakeRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)
        mw._mark_key("new-key", _make_message())
        assert mw._local_is_dup("new-key") is True

    def test_mark_key_redis_error_fallback_true(self) -> None:
        """_mark_key returns True on Redis error when fallback_on_redis_error=True."""
        mw = DeduplicationMiddleware(
            redis_client=_ErrorRedis(),
            config=DeduplicationConfig(fallback_on_redis_error=True),
        )
        assert mw._mark_key("any-key", _make_message()) is True

    def test_mark_key_redis_error_fallback_false(self) -> None:
        """_mark_key re-raises on Redis error when fallback_on_redis_error=False."""
        mw = DeduplicationMiddleware(
            redis_client=_ErrorRedis(),
            config=DeduplicationConfig(fallback_on_redis_error=False),
        )
        with pytest.raises(ConnectionError):
            mw._mark_key("any-key", _make_message())

    async def test_mark_key_async_returns_true_for_new_key(self) -> None:
        """_mark_key_async returns True for a new key."""
        redis = _FakeAsyncRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)
        assert await mw._mark_key_async("fresh", _make_message()) is True

    async def test_mark_key_async_returns_false_for_local_dup(self) -> None:
        """_mark_key_async returns False immediately for local cache hit."""
        redis = _FakeAsyncRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)
        mw._local_mark("local-key")
        assert await mw._mark_key_async("local-key", _make_message()) is False

    async def test_mark_key_async_redis_error_fallback_true(self) -> None:
        """_mark_key_async returns True on Redis error with fallback=True."""
        mw = DeduplicationMiddleware(
            redis_client=_AsyncErrorRedis(),
            config=DeduplicationConfig(fallback_on_redis_error=True),
        )
        assert await mw._mark_key_async("k", _make_message()) is True

    async def test_mark_key_async_redis_error_fallback_false(self) -> None:
        """_mark_key_async re-raises on Redis error with fallback=False."""
        mw = DeduplicationMiddleware(
            redis_client=_AsyncErrorRedis(),
            config=DeduplicationConfig(fallback_on_redis_error=False),
        )
        with pytest.raises(ConnectionError):
            await mw._mark_key_async("k", _make_message())


# ── on_start mark_policy ─────────────────────────────────────────────────


class TestOnStartPolicy:
    def test_on_start_new_message_passes_through(self) -> None:
        """on_start policy: new message is processed normally."""
        redis = _FakeRedis()
        config = DeduplicationConfig(mark_policy="on_start")
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg = _make_message(message_id="start-new")
        handler = MagicMock(return_value="ok")
        result = mw.consume_scope(handler, msg)

        handler.assert_called_once_with(msg)
        assert result == "ok"

    def test_on_start_duplicate_acked_and_skipped(self) -> None:
        """on_start policy: duplicate message is acked and handler is not called."""
        redis = _FakeRedis()
        config = DeduplicationConfig(mark_policy="on_start")
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg1 = _make_message(message_id="start-dup")
        msg1._ack_fn = MagicMock()
        mw.consume_scope(MagicMock(return_value=None), msg1)

        # Second delivery — duplicate
        msg2 = _make_message(message_id="start-dup")
        msg2._ack_fn = MagicMock()
        handler = MagicMock()
        result = mw.consume_scope(handler, msg2)

        assert result is None
        handler.assert_not_called()
        msg2._ack_fn.assert_called_once()

    def test_on_start_duplicate_already_settled_not_double_acked(self) -> None:
        """on_start policy: already-settled duplicate is not re-acked."""
        redis = _FakeRedis()
        config = DeduplicationConfig(mark_policy="on_start")
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg1 = _make_message(message_id="start-settled")
        msg1._ack_fn = MagicMock()
        mw.consume_scope(MagicMock(return_value=None), msg1)

        msg2 = _make_message(message_id="start-settled")
        msg2._ack_fn = MagicMock()
        msg2._disposition = "acked"

        mw.consume_scope(MagicMock(), msg2)
        msg2._ack_fn.assert_not_called()

    async def test_on_start_async_new_passes_through(self) -> None:
        """on_start async: new message is processed normally."""
        redis = _FakeAsyncRedis()
        config = DeduplicationConfig(mark_policy="on_start")
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg = _make_message(message_id="async-start-new")

        async def handler(m: RabbitMessage) -> str:
            return "async-ok"

        result = await mw.consume_scope_async(handler, msg)
        assert result == "async-ok"

    async def test_on_start_async_duplicate_acked_and_skipped(self) -> None:
        """on_start async: duplicate message is acked and handler not called."""
        redis = _FakeAsyncRedis()
        config = DeduplicationConfig(mark_policy="on_start")
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg1 = _make_message(message_id="async-start-dup")

        async def handler(m: RabbitMessage) -> str:
            return "first"

        await mw.consume_scope_async(handler, msg1)

        msg2 = _make_message(message_id="async-start-dup")
        ack_called = False

        async def async_ack() -> None:
            nonlocal ack_called
            ack_called = True

        msg2._ack_async_fn = async_ack
        call_count = 0

        async def handler2(m: RabbitMessage) -> str:
            nonlocal call_count
            call_count += 1
            return "second"

        result = await mw.consume_scope_async(handler2, msg2)
        assert result is None
        assert call_count == 0
        assert ack_called is True

    async def test_on_start_async_duplicate_already_settled(self) -> None:
        """on_start async: already-settled duplicate is not re-acked."""
        redis = _FakeAsyncRedis()
        config = DeduplicationConfig(mark_policy="on_start")
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg1 = _make_message(message_id="async-start-settled")

        async def _first(m: RabbitMessage) -> None:
            return None

        await mw.consume_scope_async(_first, msg1)

        msg2 = _make_message(message_id="async-start-settled")
        msg2._ack_async_fn = MagicMock()
        msg2._disposition = "acked"

        await mw.consume_scope_async(MagicMock(), msg2)
        msg2._ack_async_fn.assert_not_called()


# ── on_success local cache short-circuit ────────────────────────────────


class TestOnSuccessLocalCache:
    def test_local_cache_short_circuits_redis(self) -> None:
        """on_success: local cache hit skips Redis and acks/skips the message."""
        redis = _FakeRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        # Process first message — seeds local cache
        msg1 = _make_message(message_id="cached-msg")
        msg1._ack_fn = MagicMock()
        mw.consume_scope(MagicMock(return_value=None), msg1)

        initial_set_calls = len(redis.set_calls)

        # Second message with same id — should hit local cache, skip Redis
        msg2 = _make_message(message_id="cached-msg")
        msg2._ack_fn = MagicMock()
        handler = MagicMock()
        result = mw.consume_scope(handler, msg2)

        assert result is None
        handler.assert_not_called()
        msg2._ack_fn.assert_called_once()
        # No extra Redis calls
        assert len(redis.set_calls) == initial_set_calls

    def test_local_cache_short_circuit_already_settled(self) -> None:
        """on_success local cache: already-settled message is not re-acked."""
        redis = _FakeRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg1 = _make_message(message_id="lc-settled")
        msg1._ack_fn = MagicMock()
        mw.consume_scope(MagicMock(return_value=None), msg1)

        msg2 = _make_message(message_id="lc-settled")
        msg2._ack_fn = MagicMock()
        msg2._disposition = "acked"

        mw.consume_scope(MagicMock(), msg2)
        msg2._ack_fn.assert_not_called()

    async def test_local_cache_short_circuits_redis_async(self) -> None:
        """on_success async: local cache hit skips Redis and acks/skips."""
        redis = _FakeAsyncRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg1 = _make_message(message_id="async-lc-msg")

        async def handler(m: RabbitMessage) -> str:
            return "done"

        await mw.consume_scope_async(handler, msg1)
        initial_set_calls = len(redis.set_calls)

        msg2 = _make_message(message_id="async-lc-msg")
        ack_called = False

        async def async_ack() -> None:
            nonlocal ack_called
            ack_called = True

        msg2._ack_async_fn = async_ack
        call_count = 0

        async def handler2(m: RabbitMessage) -> str:
            nonlocal call_count
            call_count += 1
            return "should not run"

        result = await mw.consume_scope_async(handler2, msg2)
        assert result is None
        assert call_count == 0
        assert ack_called is True
        assert len(redis.set_calls) == initial_set_calls

    async def test_local_cache_short_circuit_already_settled_async(self) -> None:
        """on_success async local cache: already-settled message not re-acked."""
        redis = _FakeAsyncRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg1 = _make_message(message_id="async-lc-settled")

        async def handler(m: RabbitMessage) -> str:
            return "done"

        await mw.consume_scope_async(handler, msg1)

        msg2 = _make_message(message_id="async-lc-settled")
        msg2._ack_async_fn = MagicMock()
        msg2._disposition = "acked"

        await mw.consume_scope_async(MagicMock(), msg2)
        msg2._ack_async_fn.assert_not_called()


# ── handler failure → key cleanup ────────────────────────────────────────


class _DeleteTrackingRedis(_FakeRedis):
    """FakeRedis that also tracks delete calls."""

    def __init__(self) -> None:
        super().__init__()
        self.delete_calls: list[str] = []

    def delete(self, key: str) -> None:
        self.delete_calls.append(key)
        self._store.pop(key, None)


class _DeleteErrorRedis(_DeleteTrackingRedis):
    """FakeRedis whose delete always raises."""

    def delete(self, key: str) -> None:
        raise ConnectionError("delete failed")


class _AsyncDeleteTrackingRedis(_FakeAsyncRedis):
    """Async FakeRedis that tracks delete calls."""

    def __init__(self) -> None:
        super().__init__()
        self.delete_calls: list[str] = []

    async def delete(self, key: str) -> None:
        self.delete_calls.append(key)
        self._store.pop(key, None)


class _AsyncDeleteErrorRedis(_FakeAsyncRedis):
    """Async FakeRedis whose delete always raises."""

    async def delete(self, key: str) -> None:
        raise ConnectionError("async delete failed")


class TestHandlerFailureLeavesNoMark:
    """on_success: the key is written only AFTER handler success, so a failed
    handler leaves nothing in Redis — no mark, no cleanup delete — and the
    redelivery is processed for real."""

    def test_handler_failure_leaves_no_mark_and_redelivery_processes(self) -> None:
        redis = _DeleteTrackingRedis()
        mw = DeduplicationMiddleware(redis_client=redis)

        msg = _make_message(message_id="fail-msg")
        key = mw._extract_key(msg)

        def failing_handler(m: RabbitMessage) -> str:
            raise ValueError("handler error")

        with pytest.raises(ValueError, match="handler error"):
            mw.consume_scope(failing_handler, msg)

        assert key not in redis._store  # nothing was marked
        assert redis.delete_calls == []  # and nothing needed cleanup

        # Redelivery is processed, not dropped as a duplicate
        msg2 = _make_message(message_id="fail-msg")
        handler = MagicMock(return_value="processed")
        assert mw.consume_scope(handler, msg2) == "processed"
        handler.assert_called_once()

    async def test_async_handler_failure_leaves_no_mark_and_redelivery_processes(self) -> None:
        redis = _AsyncDeleteTrackingRedis()
        mw = DeduplicationMiddleware(redis_client=redis)

        msg = _make_message(message_id="async-fail-msg")
        key = mw._extract_key(msg)

        async def failing_handler(m: RabbitMessage) -> str:
            raise ValueError("async handler error")

        with pytest.raises(ValueError, match="async handler error"):
            await mw.consume_scope_async(failing_handler, msg)

        assert key not in redis._store
        assert redis.delete_calls == []

        msg2 = _make_message(message_id="async-fail-msg")

        async def handler(m: RabbitMessage) -> str:
            return "processed"

        assert await mw.consume_scope_async(handler, msg2) == "processed"


# ── C1 regression: crash mid-handler must not lose the redelivery ─────────


class TestCrashSafety:
    """Regression for the C1 audit finding: with mark_policy="on_success" the
    dedup key must NOT be written before the handler runs. A consumer killed
    mid-handler (OOM/SIGKILL — no except/finally runs) must leave no mark, so
    the broker's redelivery of the unacked message is processed, not
    acked-and-skipped as a duplicate."""

    def test_crash_mid_handler_does_not_drop_redelivery(self) -> None:
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis)

        msg = _make_message(message_id="crash-msg")
        key = mw._extract_key(msg)

        def crashing_handler(m: RabbitMessage) -> str:
            # The core invariant: no write happened before the handler. If a
            # SIGKILL landed here, Redis must hold nothing for this key.
            assert key not in redis._store
            assert redis.set_calls == []
            # BaseException — bypasses any `except Exception` cleanup, the
            # closest in-process simulation of abrupt death.
            raise KeyboardInterrupt("simulated OOM-kill")

        with pytest.raises(KeyboardInterrupt):
            mw.consume_scope(crashing_handler, msg)

        assert key not in redis._store

        # The broker redelivers the unacked message → it must be processed
        msg2 = _make_message(message_id="crash-msg")
        handler = MagicMock(return_value="processed")
        assert mw.consume_scope(handler, msg2) == "processed"
        handler.assert_called_once()

    async def test_async_crash_mid_handler_does_not_drop_redelivery(self) -> None:
        redis = _FakeAsyncRedis()
        mw = DeduplicationMiddleware(redis_client=redis)

        msg = _make_message(message_id="async-crash-msg")
        key = mw._extract_key(msg)

        async def crashing_handler(m: RabbitMessage) -> str:
            assert key not in redis._store
            assert redis.set_calls == []
            raise KeyboardInterrupt("simulated OOM-kill")

        with pytest.raises(KeyboardInterrupt):
            await mw.consume_scope_async(crashing_handler, msg)

        assert key not in redis._store

        msg2 = _make_message(message_id="async-crash-msg")

        async def handler(m: RabbitMessage) -> str:
            return "processed"

        assert await mw.consume_scope_async(handler, msg2) == "processed"

    def test_mark_happens_only_after_handler_success(self) -> None:
        """Ordering check: zero Redis writes before/inside the handler, exactly
        one (the success mark) after it returns."""
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis)
        msg = _make_message(message_id="ordering-msg")

        def handler(m: RabbitMessage) -> str:
            assert redis.set_calls == []
            return "ok"

        assert mw.consume_scope(handler, msg) == "ok"
        assert len(redis.set_calls) == 1


# ── success-mark Redis error must never raise after the handler ran ───────


class _SetErrorRedis:
    """exists() works; set() raises — simulates Redis dying between the dedup
    check and the success-mark."""

    def exists(self, key: str) -> int:
        return 0

    def set(self, *args: object, **kwargs: object) -> None:
        raise ConnectionError("Redis died before mark")


class _AsyncSetErrorRedis:
    async def exists(self, key: str) -> int:
        return 0

    async def set(self, *args: object, **kwargs: object) -> None:
        raise ConnectionError("Redis died before mark")


class TestSuccessMarkError:
    """A Redis failure while writing the success-mark must never raise — the
    handler's side effects are already done; raising would nack → redeliver →
    a guaranteed duplicate execution. Applies even with
    fallback_on_redis_error=False."""

    @pytest.mark.parametrize("fallback", [True, False])
    def test_mark_error_after_success_returns_result(self, fallback: bool) -> None:
        mw = DeduplicationMiddleware(
            redis_client=_SetErrorRedis(),
            config=DeduplicationConfig(fallback_on_redis_error=fallback),
        )
        msg = _make_message(message_id="mark-err-msg")
        result = mw.consume_scope(MagicMock(return_value="done"), msg)
        assert result == "done"

    @pytest.mark.parametrize("fallback", [True, False])
    async def test_async_mark_error_after_success_returns_result(self, fallback: bool) -> None:
        mw = DeduplicationMiddleware(
            redis_client=_AsyncSetErrorRedis(),
            config=DeduplicationConfig(fallback_on_redis_error=fallback),
        )
        msg = _make_message(message_id="async-mark-err-msg")

        async def handler(m: RabbitMessage) -> str:
            return "done"

        assert await mw.consume_scope_async(handler, msg) == "done"


# ── on_start: sentinel cleanup delete error is logged, not raised ─────────


class TestOnStartCleanupError:
    def test_on_start_sentinel_cleanup_delete_error_logged_not_raised(self) -> None:
        redis = _DeleteErrorRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=DeduplicationConfig(mark_policy="on_start"))
        msg = _make_message(message_id="onstart-cleanup-err")

        result = mw.consume_scope(lambda m: REQUEUED_FOR_RETRY, msg)
        assert result is REQUEUED_FOR_RETRY  # delete error swallowed, sentinel intact

    async def test_on_start_async_sentinel_cleanup_delete_error_logged_not_raised(self) -> None:
        redis = _AsyncDeleteErrorRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=DeduplicationConfig(mark_policy="on_start"))
        msg = _make_message(message_id="onstart-async-cleanup-err")

        async def inner(m: RabbitMessage) -> Any:
            return REQUEUED_FOR_RETRY

        result = await mw.consume_scope_async(inner, msg)
        assert result is REQUEUED_FOR_RETRY


# ── local LRU thread safety ───────────────────────────────────────────────


class TestLocalCacheThreadSafety:
    def test_concurrent_mark_check_remove_do_not_corrupt(self) -> None:
        """The local cache is mutated from sync worker-pool daemon threads;
        concurrent mark/check/remove with eviction pressure must not raise
        (unlocked OrderedDict mutation can KeyError mid-eviction)."""
        import threading

        redis = _FakeRedis()
        config = DeduplicationConfig(local_cache_size=8)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)
        errors: list[BaseException] = []

        def hammer(worker: int) -> None:
            try:
                for i in range(2000):
                    key = f"k-{worker}-{i % 32}"
                    mw._local_mark(key)
                    mw._local_is_dup(key)
                    mw._local_remove(f"k-{worker}-{(i + 7) % 32}")
            except BaseException as exc:  # collect for assertion on the main thread
                errors.append(exc)

        threads = [threading.Thread(target=hammer, args=(w,)) for w in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert errors == []
        assert mw._local_cache is not None
        assert len(mw._local_cache) <= config.local_cache_size


# ── H8: dedup + retry composition must not silently drop the retry ────────


class TestRetryRequeueComposition:
    """H8: with an inner RetryMiddleware that swallows a transient failure
    and returns REQUEUED_FOR_RETRY instead of raising, DeduplicationMiddleware
    must NOT mark the key as processed — otherwise the later retry
    redelivery (same dedup key) is dropped as a duplicate and never actually
    processed (silent message loss)."""

    def _retry_mw_that_requeues(self) -> RetryMiddleware:
        """A RetryMiddleware whose consume_scope always "succeeds" at
        requeuing — configured with plenty of retries left so a transient
        failure never becomes terminal, and a working publish_fn."""
        from rabbitkit.core.config import RetryConfig

        return RetryMiddleware(
            RetryConfig(max_retries=3, delays=(5, 30, 120)),
            publish_fn=lambda env: PublishOutcome(status=PublishStatus.CONFIRMED),
        )

    def test_on_success_does_not_mark_key_when_retry_requeues(self) -> None:
        """H8 exact spec: [dedup(on_success), retry] — handler fails once —
        the key must NOT be marked as processed, so a second (retry)
        delivery of the SAME message is processed for real, not dropped."""
        redis = _DeleteTrackingRedis()
        mw = DeduplicationMiddleware(redis_client=redis)
        retry_mw = self._retry_mw_that_requeues()

        msg = _make_message(message_id="h8-msg")
        msg._ack_fn = MagicMock()  # RetryMiddleware acks the source on requeue
        key = mw._extract_key(msg)

        call_count = 0

        def handler(m: RabbitMessage) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionResetError("transient")  # classified TRANSIENT
            return "ok"

        # First delivery: handler fails, retry requeues (acks + delay-queue
        # publish). dedup must see this as "not done" and skip the mark.
        result1 = mw.consume_scope(lambda m: retry_mw.consume_scope(handler, m), msg)
        assert result1 is REQUEUED_FOR_RETRY
        assert call_count == 1
        assert key not in redis._store  # key was NOT left marked

        # Second delivery (the "retry"): same dedup key, message_id
        # preserved by RetryMiddleware's own envelope construction. Because
        # the key was never marked, this is NOT treated as a duplicate --
        # the handler actually runs and processes it.
        msg2 = _make_message(message_id="h8-msg")  # same key as msg
        msg2._ack_fn = MagicMock()
        result2 = mw.consume_scope(lambda m: retry_mw.consume_scope(handler, m), msg2)
        assert result2 == "ok"
        assert call_count == 2  # handler WAS called the second time -- not dropped

    async def test_on_success_async_does_not_mark_key_when_retry_requeues(self) -> None:
        redis = _AsyncDeleteTrackingRedis()
        mw = DeduplicationMiddleware(redis_client=redis)
        retry_mw = self._retry_mw_that_requeues()

        msg = _make_message(message_id="h8-async-msg")
        msg._ack_async_fn = _noop_async_ack
        key = mw._extract_key(msg)

        call_count = 0

        async def handler(m: RabbitMessage) -> str:
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                raise ConnectionResetError("transient")
            return "ok"

        async def inner1(m: RabbitMessage) -> str:
            return await retry_mw.consume_scope_async(handler, m)

        result1 = await mw.consume_scope_async(inner1, msg)
        assert result1 is REQUEUED_FOR_RETRY
        assert call_count == 1
        assert key not in redis._store

        msg2 = _make_message(message_id="h8-async-msg")
        msg2._ack_async_fn = _noop_async_ack

        async def inner2(m: RabbitMessage) -> str:
            return await retry_mw.consume_scope_async(handler, m)

        result2 = await mw.consume_scope_async(inner2, msg2)
        assert result2 == "ok"
        assert call_count == 2

    def test_on_start_undoes_premature_mark_when_retry_requeues(self) -> None:
        """on_start marks BEFORE the handler runs -- when retry signals a
        requeue, the premature mark must be retroactively undone."""
        redis = _DeleteTrackingRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=DeduplicationConfig(mark_policy="on_start"))
        retry_mw = self._retry_mw_that_requeues()

        msg = _make_message(message_id="h8-onstart-msg")
        msg._ack_fn = MagicMock()
        key = mw._extract_key(msg)

        def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("transient")

        result = mw.consume_scope(lambda m: retry_mw.consume_scope(failing_handler, m), msg)

        assert result is REQUEUED_FOR_RETRY
        assert key in redis.delete_calls
        assert key not in redis._store  # the on_start mark was undone

    async def test_on_start_async_undoes_premature_mark_when_retry_requeues(self) -> None:
        redis = _AsyncDeleteTrackingRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=DeduplicationConfig(mark_policy="on_start"))
        retry_mw = self._retry_mw_that_requeues()

        msg = _make_message(message_id="h8-onstart-async-msg")
        msg._ack_async_fn = _noop_async_ack
        key = mw._extract_key(msg)

        async def failing_handler(m: RabbitMessage) -> None:
            raise ConnectionResetError("transient")

        async def inner(m: RabbitMessage) -> Any:
            return await retry_mw.consume_scope_async(failing_handler, m)

        result = await mw.consume_scope_async(inner, msg)

        assert result is REQUEUED_FOR_RETRY
        assert key in redis.delete_calls
        assert key not in redis._store

    def test_permanent_failure_still_propagates_and_leaves_no_mark(self) -> None:
        """Sanity check: a PERMANENT (non-retryable) failure still raises
        through retry (terminal path); dedup never marked anything, so the
        message can dead-letter and be replayed without a stale dedup key."""
        redis = _DeleteTrackingRedis()
        mw = DeduplicationMiddleware(redis_client=redis)
        retry_mw = self._retry_mw_that_requeues()

        msg = _make_message(message_id="h8-permanent-msg")
        key = mw._extract_key(msg)

        def failing_handler(m: RabbitMessage) -> None:
            raise ValueError("permanent")  # classified PERMANENT -> terminal, re-raised

        with pytest.raises(ValueError, match="permanent"):
            mw.consume_scope(lambda m: retry_mw.consume_scope(failing_handler, m), msg)

        assert key not in redis._store


def test_requeued_for_retry_repr() -> None:
    assert repr(REQUEUED_FOR_RETRY) == "REQUEUED_FOR_RETRY"


# ── claim mark_policy ─────────────────────────────────────────────────────


def _claim_config(**overrides: object) -> DeduplicationConfig:
    defaults: dict[str, object] = {"mark_policy": "claim", "processing_timeout": 60, "ttl": 3600}
    defaults.update(overrides)
    return DeduplicationConfig(**defaults)  # type: ignore[arg-type]


class TestClaimPolicy:
    def test_fresh_message_claims_then_completes(self) -> None:
        """claim: in-flight written before the handler (with processing_timeout),
        completed written after success (with full ttl)."""
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config())

        msg = _make_message(message_id="claim-fresh")
        key = mw._extract_key(msg)

        def handler(m: RabbitMessage) -> str:
            # While the handler runs, the key must be a live in-flight claim
            assert redis._store[key] == "in-flight"
            return "ok"

        assert mw.consume_scope(handler, msg) == "ok"

        claim_call, complete_call = redis.set_calls
        assert claim_call == {"key": key, "value": "in-flight", "nx": True, "ex": 60}
        assert complete_call == {"key": key, "value": "completed", "nx": False, "ex": 3600}
        assert redis._store[key] == "completed"

    def test_enum_member_selects_claim(self) -> None:
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(
            redis_client=redis,
            config=DeduplicationConfig(mark_policy=DeduplicationMarkPolicy.CLAIM),
        )
        msg = _make_message(message_id="claim-enum")
        assert mw.consume_scope(MagicMock(return_value="ok"), msg) == "ok"
        assert redis.set_calls[0]["value"] == "in-flight"

    def test_in_flight_duplicate_is_nack_requeued(self) -> None:
        """A concurrent copy that sees a live claim is requeued (NOT acked),
        so it survives if the claiming consumer dies mid-handler."""
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config())

        msg = _make_message(message_id="claim-inflight")
        key = mw._extract_key(msg)
        redis._store[key] = "in-flight"  # another consumer's live claim

        msg._ack_fn = MagicMock()
        msg._nack_fn = MagicMock()
        handler = MagicMock()

        result = mw.consume_scope(handler, msg)

        assert result is None
        handler.assert_not_called()
        msg._nack_fn.assert_called_once_with(True)  # requeue=True
        msg._ack_fn.assert_not_called()

    def test_in_flight_duplicate_ack_skip_mode(self) -> None:
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config(on_in_flight="ack_skip"))

        msg = _make_message(message_id="claim-ackskip")
        redis._store[mw._extract_key(msg)] = "in-flight"
        msg._ack_fn = MagicMock()
        msg._nack_fn = MagicMock()

        assert mw.consume_scope(MagicMock(), msg) is None
        msg._ack_fn.assert_called_once()
        msg._nack_fn.assert_not_called()

    def test_completed_duplicate_is_acked_and_skipped(self) -> None:
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config())

        msg = _make_message(message_id="claim-completed")
        redis._store[mw._extract_key(msg)] = "completed"
        msg._ack_fn = MagicMock()
        handler = MagicMock()

        assert mw.consume_scope(handler, msg) is None
        handler.assert_not_called()
        msg._ack_fn.assert_called_once()

    def test_legacy_on_success_value_treated_as_completed(self) -> None:
        """A key written as "1" by an on_success/on_start deployment must read
        as completed, so switching an existing deployment to claim is safe."""
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config())

        msg = _make_message(message_id="claim-legacy")
        redis._store[mw._extract_key(msg)] = "1"
        msg._ack_fn = MagicMock()
        handler = MagicMock()

        assert mw.consume_scope(handler, msg) is None
        handler.assert_not_called()
        msg._ack_fn.assert_called_once()

    def test_bytes_in_flight_value_recognized(self) -> None:
        """Real redis clients (decode_responses=False) return bytes from GET."""
        assert DeduplicationMiddleware._is_in_flight(b"in-flight") is True
        assert DeduplicationMiddleware._is_in_flight(b"completed") is False
        assert DeduplicationMiddleware._is_in_flight(b"1") is False
        assert DeduplicationMiddleware._is_in_flight("in-flight") is True
        # None = claim expired between the failed SET NX and the GET —
        # treated as in-flight so the requeued copy re-claims cleanly.
        assert DeduplicationMiddleware._is_in_flight(None) is True

    def test_handler_failure_releases_claim_and_redelivery_reprocesses(self) -> None:
        redis = _DeleteTrackingRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config())

        msg = _make_message(message_id="claim-fail")
        key = mw._extract_key(msg)

        def failing_handler(m: RabbitMessage) -> str:
            raise ValueError("handler error")

        with pytest.raises(ValueError, match="handler error"):
            mw.consume_scope(failing_handler, msg)

        assert key in redis.delete_calls  # claim released for immediate retry
        assert key not in redis._store

        msg2 = _make_message(message_id="claim-fail")
        handler = MagicMock(return_value="processed")
        assert mw.consume_scope(handler, msg2) == "processed"

    def test_retry_sentinel_releases_claim_without_completing(self) -> None:
        redis = _DeleteTrackingRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config())

        msg = _make_message(message_id="claim-sentinel")
        key = mw._extract_key(msg)

        result = mw.consume_scope(lambda m: REQUEUED_FOR_RETRY, msg)

        assert result is REQUEUED_FOR_RETRY
        assert key in redis.delete_calls
        assert key not in redis._store

    def test_crash_mid_handler_leaves_only_expiring_claim(self) -> None:
        """C1-equivalent for claim: a crash mid-handler leaves ONLY the
        in-flight claim (which Redis expires after processing_timeout) —
        never a completed mark, so the redelivery is eventually processed."""
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config())

        msg = _make_message(message_id="claim-crash")
        key = mw._extract_key(msg)

        def crashing_handler(m: RabbitMessage) -> str:
            raise KeyboardInterrupt("simulated OOM-kill")  # BaseException: no cleanup

        with pytest.raises(KeyboardInterrupt):
            mw.consume_scope(crashing_handler, msg)

        assert redis._store[key] == "in-flight"  # not completed
        assert redis.set_calls[0]["ex"] == 60  # bounded by processing_timeout

        # Simulate the claim expiring, then the broker redelivering
        del redis._store[key]
        msg2 = _make_message(message_id="claim-crash")
        handler = MagicMock(return_value="processed")
        assert mw.consume_scope(handler, msg2) == "processed"

    def test_completed_flip_error_never_raises(self) -> None:
        """Redis dying between handler success and the completed-flip must not
        raise — the handler's side effects are committed."""

        class _FlipErrorRedis(_FakeRedis):
            def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool | None:
                if value == "completed":
                    raise ConnectionError("Redis died before completed-flip")
                return super().set(key, value, nx=nx, ex=ex)

        mw = DeduplicationMiddleware(
            redis_client=_FlipErrorRedis(),
            config=_claim_config(fallback_on_redis_error=False),
        )
        msg = _make_message(message_id="claim-flip-err")
        assert mw.consume_scope(MagicMock(return_value="done"), msg) == "done"

    def test_claim_redis_error_fallback_and_fail_closed(self) -> None:
        mw_open = DeduplicationMiddleware(
            redis_client=_ErrorRedis(), config=_claim_config(fallback_on_redis_error=True)
        )
        msg = _make_message(message_id="claim-redis-err")
        assert mw_open.consume_scope(MagicMock(return_value="ok"), msg) == "ok"

        mw_closed = DeduplicationMiddleware(
            redis_client=_ErrorRedis(), config=_claim_config(fallback_on_redis_error=False)
        )
        with pytest.raises(ConnectionError, match="Redis is down"):
            mw_closed.consume_scope(MagicMock(), _make_message(message_id="claim-redis-err-2"))

    def test_local_cache_short_circuits_claim(self) -> None:
        redis = _FakeRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config(local_cache_size=10))

        msg1 = _make_message(message_id="claim-lc")
        mw.consume_scope(MagicMock(return_value="ok"), msg1)
        calls_before = len(redis.set_calls)

        msg2 = _make_message(message_id="claim-lc")
        msg2._ack_fn = MagicMock()
        handler = MagicMock()
        assert mw.consume_scope(handler, msg2) is None
        handler.assert_not_called()
        msg2._ack_fn.assert_called_once()
        assert len(redis.set_calls) == calls_before  # Redis never touched

    # ── async variants ────────────────────────────────────────────────────

    async def test_async_fresh_message_claims_then_completes(self) -> None:
        redis = _FakeAsyncRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config())

        msg = _make_message(message_id="async-claim-fresh")
        key = mw._extract_key(msg)

        async def handler(m: RabbitMessage) -> str:
            assert redis._store[key] == "in-flight"
            return "ok"

        assert await mw.consume_scope_async(handler, msg) == "ok"
        assert redis._store[key] == "completed"
        assert redis.set_calls[0]["ex"] == 60
        assert redis.set_calls[1]["ex"] == 3600

    async def test_async_in_flight_duplicate_is_nack_requeued(self) -> None:
        redis = _FakeAsyncRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config())

        msg = _make_message(message_id="async-claim-inflight")
        redis._store[mw._extract_key(msg)] = "in-flight"

        nack_args: list[bool] = []

        async def nack_fn(requeue: bool) -> None:
            nack_args.append(requeue)

        msg._nack_async_fn = nack_fn
        call_count = 0

        async def handler(m: RabbitMessage) -> str:
            nonlocal call_count
            call_count += 1
            return "should not run"

        assert await mw.consume_scope_async(handler, msg) is None
        assert call_count == 0
        assert nack_args == [True]

    async def test_async_in_flight_duplicate_ack_skip_mode(self) -> None:
        redis = _FakeAsyncRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config(on_in_flight="ack_skip"))

        msg = _make_message(message_id="async-claim-ackskip")
        redis._store[mw._extract_key(msg)] = "in-flight"

        ack_called = False

        async def ack_fn() -> None:
            nonlocal ack_called
            ack_called = True

        msg._ack_async_fn = ack_fn

        assert await mw.consume_scope_async(MagicMock(), msg) is None
        assert ack_called is True

    async def test_async_completed_duplicate_is_acked_and_skipped(self) -> None:
        redis = _FakeAsyncRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config())

        msg = _make_message(message_id="async-claim-completed")
        redis._store[mw._extract_key(msg)] = "completed"

        ack_called = False

        async def ack_fn() -> None:
            nonlocal ack_called
            ack_called = True

        msg._ack_async_fn = ack_fn

        assert await mw.consume_scope_async(MagicMock(), msg) is None
        assert ack_called is True

    async def test_async_handler_failure_releases_claim(self) -> None:
        redis = _AsyncDeleteTrackingRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config())

        msg = _make_message(message_id="async-claim-fail")
        key = mw._extract_key(msg)

        async def failing_handler(m: RabbitMessage) -> str:
            raise ValueError("async handler error")

        with pytest.raises(ValueError, match="async handler error"):
            await mw.consume_scope_async(failing_handler, msg)

        assert key in redis.delete_calls
        assert key not in redis._store

    async def test_async_retry_sentinel_releases_claim(self) -> None:
        redis = _AsyncDeleteTrackingRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config())

        msg = _make_message(message_id="async-claim-sentinel")
        key = mw._extract_key(msg)

        async def inner(m: RabbitMessage) -> Any:
            return REQUEUED_FOR_RETRY

        result = await mw.consume_scope_async(inner, msg)
        assert result is REQUEUED_FOR_RETRY
        assert key in redis.delete_calls
        assert key not in redis._store

    async def test_async_completed_flip_error_never_raises(self) -> None:
        class _AsyncFlipErrorRedis(_FakeAsyncRedis):
            async def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool | None:
                if value == "completed":
                    raise ConnectionError("Redis died before completed-flip")
                return await super().set(key, value, nx=nx, ex=ex)

        mw = DeduplicationMiddleware(
            redis_client=_AsyncFlipErrorRedis(),
            config=_claim_config(fallback_on_redis_error=False),
        )
        msg = _make_message(message_id="async-claim-flip-err")

        async def handler(m: RabbitMessage) -> str:
            return "done"

        assert await mw.consume_scope_async(handler, msg) == "done"

    async def test_async_claim_redis_error_fallback(self) -> None:
        mw = DeduplicationMiddleware(
            redis_client=_AsyncErrorRedis(), config=_claim_config(fallback_on_redis_error=True)
        )
        msg = _make_message(message_id="async-claim-redis-err")

        async def handler(m: RabbitMessage) -> str:
            return "ok"

        assert await mw.consume_scope_async(handler, msg) == "ok"

    async def test_async_local_cache_short_circuits_claim(self) -> None:
        redis = _FakeAsyncRedis()
        mw = DeduplicationMiddleware(redis_client=redis, config=_claim_config(local_cache_size=10))

        msg1 = _make_message(message_id="async-claim-lc")

        async def handler(m: RabbitMessage) -> str:
            return "ok"

        await mw.consume_scope_async(handler, msg1)
        calls_before = len(redis.set_calls)

        msg2 = _make_message(message_id="async-claim-lc")
        ack_called = False

        async def ack_fn() -> None:
            nonlocal ack_called
            ack_called = True

        msg2._ack_async_fn = ack_fn

        assert await mw.consume_scope_async(handler, msg2) is None
        assert ack_called is True
        assert len(redis.set_calls) == calls_before

    async def test_async_claim_redis_error_fail_closed_raises(self) -> None:
        mw = DeduplicationMiddleware(
            redis_client=_AsyncErrorRedis(), config=_claim_config(fallback_on_redis_error=False)
        )

        async def handler(m: RabbitMessage) -> str:
            return "unreached"

        with pytest.raises(ConnectionError, match="Redis is down"):
            await mw.consume_scope_async(handler, _make_message(message_id="async-claim-closed"))

    def test_get_error_after_failed_claim_fallback_and_fail_closed(self) -> None:
        """SET NX says the key exists, then GET fails: fallback processes,
        fail-closed raises."""

        class _GetErrorRedis(_FakeRedis):
            def get(self, key: str) -> str | None:
                raise ConnectionError("Redis is down")

        redis = _GetErrorRedis()
        msg = _make_message(message_id="claim-get-err")
        mw_open = DeduplicationMiddleware(redis_client=redis, config=_claim_config(fallback_on_redis_error=True))
        redis._store[mw_open._extract_key(msg)] = "in-flight"

        assert mw_open.consume_scope(MagicMock(return_value="ok"), msg) == "ok"

        mw_closed = DeduplicationMiddleware(redis_client=redis, config=_claim_config(fallback_on_redis_error=False))
        with pytest.raises(ConnectionError, match="Redis is down"):
            mw_closed.consume_scope(MagicMock(), _make_message(message_id="claim-get-err"))

    async def test_async_get_error_after_failed_claim_fallback_and_fail_closed(self) -> None:
        class _AsyncGetErrorRedis(_FakeAsyncRedis):
            async def get(self, key: str) -> str | None:
                raise ConnectionError("Redis is down")

        redis = _AsyncGetErrorRedis()
        msg = _make_message(message_id="async-claim-get-err")
        mw_open = DeduplicationMiddleware(redis_client=redis, config=_claim_config(fallback_on_redis_error=True))
        redis._store[mw_open._extract_key(msg)] = "in-flight"

        async def handler(m: RabbitMessage) -> str:
            return "ok"

        assert await mw_open.consume_scope_async(handler, msg) == "ok"

        mw_closed = DeduplicationMiddleware(redis_client=redis, config=_claim_config(fallback_on_redis_error=False))
        with pytest.raises(ConnectionError, match="Redis is down"):
            await mw_closed.consume_scope_async(handler, _make_message(message_id="async-claim-get-err"))
