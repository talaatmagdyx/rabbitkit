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

    def test_none_message_id_uses_empty_string(self) -> None:
        """If message_id is None, key uses empty string."""
        redis = _FakeRedis()
        config = DeduplicationConfig(key_source="message_id", key_prefix="test")
        mw = DeduplicationMiddleware(redis_client=redis, config=config)

        msg = _make_message(message_id=None)
        mw.consume_scope(MagicMock(), msg)

        assert redis.set_calls[0]["key"] == "test:"


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
