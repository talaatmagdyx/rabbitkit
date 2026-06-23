"""Tests for results/ — RedisResultBackend and ResultMiddleware."""

from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.results.backend import RedisResultBackend, ResultBackend
from rabbitkit.results.middleware import ResultMiddleware

# ── helpers ───────────────────────────────────────────────────────────────


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {"body": b'{"id": 1}', "routing_key": "test.key"}
    defaults.update(kwargs)
    return RabbitMessage(**defaults)  # type: ignore[arg-type]


# ── RedisResultBackend ───────────────────────────────────────────────────


class TestRedisResultBackend:
    def test_store_calls_redis_set_with_ttl(self) -> None:
        redis = MagicMock()
        backend = RedisResultBackend(redis, key_prefix="rk:res:")
        backend.store("corr-1", b"result-data", ttl=600)
        redis.set.assert_called_once_with("rk:res:corr-1", b"result-data", ex=600)

    def test_fetch_calls_redis_get(self) -> None:
        redis = MagicMock()
        redis.get.return_value = b"stored"
        backend = RedisResultBackend(redis)
        result = backend.fetch("corr-2")
        redis.get.assert_called_once_with("rabbitkit:result:corr-2")
        assert result == b"stored"

    def test_fetch_returns_none_when_missing(self) -> None:
        redis = MagicMock()
        redis.get.return_value = None
        backend = RedisResultBackend(redis)
        result = backend.fetch("missing")
        assert result is None

    def test_default_key_prefix(self) -> None:
        redis = MagicMock()
        backend = RedisResultBackend(redis)
        backend.store("abc", b"data")
        redis.set.assert_called_once_with("rabbitkit:result:abc", b"data", ex=3600)

    @pytest.mark.asyncio
    async def test_store_async(self) -> None:
        redis = AsyncMock()
        backend = RedisResultBackend(redis)
        await backend.store_async("corr-3", b"async-data", ttl=900)
        redis.set.assert_awaited_once_with("rabbitkit:result:corr-3", b"async-data", ex=900)

    @pytest.mark.asyncio
    async def test_fetch_async(self) -> None:
        redis = AsyncMock()
        redis.get.return_value = b"async-stored"
        backend = RedisResultBackend(redis)
        result = await backend.fetch_async("corr-4")
        redis.get.assert_awaited_once_with("rabbitkit:result:corr-4")
        assert result == b"async-stored"


class TestResultBackendProtocol:
    def test_redis_backend_satisfies_protocol(self) -> None:
        redis = MagicMock()
        backend = RedisResultBackend(redis)
        assert isinstance(backend, ResultBackend)


# ── ResultMiddleware ─────────────────────────────────────────────────────


class TestResultMiddleware:
    def test_store_called_on_handler_return(self) -> None:
        backend = MagicMock()
        mw = ResultMiddleware(backend, ttl=120)
        msg = _make_message(correlation_id="corr-100")

        def call_next(m: RabbitMessage) -> dict:
            return {"status": "ok"}

        result = mw.consume_scope(call_next, msg)

        assert result == {"status": "ok"}
        backend.store.assert_called_once()
        args = backend.store.call_args
        assert args[0][0] == "corr-100"
        assert json.loads(args[0][1]) == {"status": "ok"}
        assert args[0][2] == 120

    def test_skipped_on_none_return(self) -> None:
        backend = MagicMock()
        mw = ResultMiddleware(backend)
        msg = _make_message(correlation_id="corr-200")

        def call_next(m: RabbitMessage) -> None:
            return None

        result = mw.consume_scope(call_next, msg)

        assert result is None
        backend.store.assert_not_called()

    def test_skipped_without_correlation_id(self) -> None:
        backend = MagicMock()
        mw = ResultMiddleware(backend)
        msg = _make_message(correlation_id=None)

        def call_next(m: RabbitMessage) -> dict:
            return {"data": 1}

        result = mw.consume_scope(call_next, msg)

        assert result == {"data": 1}
        backend.store.assert_not_called()

    def test_bytes_result_passthrough(self) -> None:
        backend = MagicMock()
        mw = ResultMiddleware(backend)
        msg = _make_message(correlation_id="corr-300")

        def call_next(m: RabbitMessage) -> bytes:
            return b"raw-bytes"

        mw.consume_scope(call_next, msg)

        args = backend.store.call_args
        assert args[0][1] == b"raw-bytes"

    def test_custom_serializer_used(self) -> None:
        backend = MagicMock()
        serializer = MagicMock()
        serializer.encode.return_value = b"custom-encoded"
        mw = ResultMiddleware(backend, serializer=serializer)
        msg = _make_message(correlation_id="corr-400")

        def call_next(m: RabbitMessage) -> dict:
            return {"key": "value"}

        mw.consume_scope(call_next, msg)

        serializer.encode.assert_called_once_with({"key": "value"})
        args = backend.store.call_args
        assert args[0][1] == b"custom-encoded"

    @pytest.mark.asyncio
    async def test_async_store_called_on_handler_return(self) -> None:
        backend = AsyncMock()
        mw = ResultMiddleware(backend, ttl=300)
        msg = _make_message(correlation_id="corr-500")

        async def call_next(m: RabbitMessage) -> dict:
            return {"async": True}

        result = await mw.consume_scope_async(call_next, msg)

        assert result == {"async": True}
        backend.store_async.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_async_skipped_on_none_return(self) -> None:
        backend = AsyncMock()
        mw = ResultMiddleware(backend)
        msg = _make_message(correlation_id="corr-600")

        async def call_next(m: RabbitMessage) -> None:
            return None

        result = await mw.consume_scope_async(call_next, msg)

        assert result is None
        backend.store_async.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_async_skipped_without_correlation_id(self) -> None:
        backend = AsyncMock()
        mw = ResultMiddleware(backend)
        msg = _make_message(correlation_id=None)

        async def call_next(m: RabbitMessage) -> dict:
            return {"data": 1}

        result = await mw.consume_scope_async(call_next, msg)

        assert result == {"data": 1}
        backend.store_async.assert_not_awaited()
