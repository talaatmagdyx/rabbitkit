"""Tests for middleware/deduplication.py — DeduplicationMiddleware."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from rabbitkit.core.config import DeduplicationConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.middleware.deduplication import DeduplicationMiddleware

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


class _FakeRedis:
    """In-memory Redis mock for sync dedup tests."""

    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self.set_calls: list[dict] = []

    def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool | None:
        self.set_calls.append({"key": key, "value": value, "nx": nx, "ex": ex})
        if nx and key in self._store:
            return None  # key exists → duplicate
        self._store[key] = value
        return True  # key was set → new

    def clear(self) -> None:
        self._store.clear()
        self.set_calls.clear()


class _FakeAsyncRedis:
    """In-memory async Redis mock for async dedup tests."""

    def __init__(self) -> None:
        self._store: dict[str, str] = {}
        self.set_calls: list[dict] = []

    async def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool | None:
        self.set_calls.append({"key": key, "value": value, "nx": nx, "ex": ex})
        if nx and key in self._store:
            return None
        self._store[key] = value
        return True


class _ErrorRedis:
    """Redis mock that always raises on set."""

    def set(self, *args: object, **kwargs: object) -> None:
        raise ConnectionError("Redis is down")


class _AsyncErrorRedis:
    """Async Redis mock that always raises on set."""

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
        assert mw._mark_key("fresh-key") is True

    def test_mark_key_returns_false_for_local_dup(self) -> None:
        """_mark_key returns False immediately when key is in local cache."""
        redis = _FakeRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)
        mw._local_mark("cached-key")
        assert mw._mark_key("cached-key") is False
        # Redis should NOT have been called
        assert len(redis.set_calls) == 0

    def test_mark_key_returns_false_for_redis_dup(self) -> None:
        """_mark_key returns False when Redis SET nx fails (already exists)."""
        redis = _FakeRedis()
        # Pre-seed the Redis store
        redis._store["taken-key"] = "1"
        mw = DeduplicationMiddleware(redis_client=redis)
        assert mw._mark_key("taken-key") is False

    def test_mark_key_populates_local_cache_on_success(self) -> None:
        """_mark_key records key in local cache after successful Redis SET."""
        redis = _FakeRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)
        mw._mark_key("new-key")
        assert mw._local_is_dup("new-key") is True

    def test_mark_key_redis_error_fallback_true(self) -> None:
        """_mark_key returns True on Redis error when fallback_on_redis_error=True."""
        mw = DeduplicationMiddleware(
            redis_client=_ErrorRedis(),
            config=DeduplicationConfig(fallback_on_redis_error=True),
        )
        assert mw._mark_key("any-key") is True

    def test_mark_key_redis_error_fallback_false(self) -> None:
        """_mark_key re-raises on Redis error when fallback_on_redis_error=False."""
        mw = DeduplicationMiddleware(
            redis_client=_ErrorRedis(),
            config=DeduplicationConfig(fallback_on_redis_error=False),
        )
        with pytest.raises(ConnectionError):
            mw._mark_key("any-key")

    async def test_mark_key_async_returns_true_for_new_key(self) -> None:
        """_mark_key_async returns True for a new key."""
        redis = _FakeAsyncRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)
        assert await mw._mark_key_async("fresh") is True

    async def test_mark_key_async_returns_false_for_local_dup(self) -> None:
        """_mark_key_async returns False immediately for local cache hit."""
        redis = _FakeAsyncRedis()
        config = DeduplicationConfig(local_cache_size=10)
        mw = DeduplicationMiddleware(redis_client=redis, config=config)
        mw._local_mark("local-key")
        assert await mw._mark_key_async("local-key") is False

    async def test_mark_key_async_redis_error_fallback_true(self) -> None:
        """_mark_key_async returns True on Redis error with fallback=True."""
        mw = DeduplicationMiddleware(
            redis_client=_AsyncErrorRedis(),
            config=DeduplicationConfig(fallback_on_redis_error=True),
        )
        assert await mw._mark_key_async("k") is True

    async def test_mark_key_async_redis_error_fallback_false(self) -> None:
        """_mark_key_async re-raises on Redis error with fallback=False."""
        mw = DeduplicationMiddleware(
            redis_client=_AsyncErrorRedis(),
            config=DeduplicationConfig(fallback_on_redis_error=False),
        )
        with pytest.raises(ConnectionError):
            await mw._mark_key_async("k")


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


class TestHandlerFailureKeyCleanup:
    def test_handler_failure_deletes_key(self) -> None:
        """on_success: when handler raises, the dedup key is deleted from Redis."""
        redis = _DeleteTrackingRedis()
        mw = DeduplicationMiddleware(redis_client=redis)

        msg = _make_message(message_id="fail-msg")
        key = mw._extract_key(msg)

        def failing_handler(m: RabbitMessage) -> str:
            raise ValueError("handler error")

        with pytest.raises(ValueError, match="handler error"):
            mw.consume_scope(failing_handler, msg)

        assert key in redis.delete_calls

    def test_handler_failure_delete_error_logged_not_raised(self) -> None:
        """on_success: when handler fails AND Redis delete fails, error is logged (not re-raised)."""
        redis = _DeleteErrorRedis()
        mw = DeduplicationMiddleware(redis_client=redis)

        msg = _make_message(message_id="fail-delete-msg")

        def failing_handler(m: RabbitMessage) -> str:
            raise ValueError("handler error")

        # The original handler error should propagate, not the delete error
        with pytest.raises(ValueError, match="handler error"):
            mw.consume_scope(failing_handler, msg)

    async def test_async_handler_failure_deletes_key(self) -> None:
        """on_success async: when handler raises, the dedup key is deleted from Redis."""
        redis = _AsyncDeleteTrackingRedis()
        mw = DeduplicationMiddleware(redis_client=redis)

        msg = _make_message(message_id="async-fail-msg")
        key = mw._extract_key(msg)

        async def failing_handler(m: RabbitMessage) -> str:
            raise ValueError("async handler error")

        with pytest.raises(ValueError, match="async handler error"):
            await mw.consume_scope_async(failing_handler, msg)

        assert key in redis.delete_calls

    async def test_async_handler_failure_delete_error_logged_not_raised(self) -> None:
        """on_success async: delete error after handler failure is logged, not re-raised."""
        redis = _AsyncDeleteErrorRedis()
        mw = DeduplicationMiddleware(redis_client=redis)

        msg = _make_message(message_id="async-fail-delete-msg")

        async def failing_handler(m: RabbitMessage) -> str:
            raise ValueError("async handler error")

        with pytest.raises(ValueError, match="async handler error"):
            await mw.consume_scope_async(failing_handler, msg)
