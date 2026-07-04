"""Tests for AsyncBroker.request() shorthand (F4)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.config import RabbitConfig


class TestAsyncBrokerRequest:

    def test_request_before_start_raises(self) -> None:
        broker = AsyncBroker(RabbitConfig())
        with pytest.raises(RuntimeError, match="Broker not started"):
            import asyncio

            # asyncio.run (not get_event_loop) — the module-level loop state
            # depends on which pytest-asyncio tests ran before this one.
            asyncio.run(broker.request("test.queue", b"body"))

    @pytest.mark.asyncio
    async def test_request_creates_rpc_client(self) -> None:
        broker = AsyncBroker(RabbitConfig())
        broker._transport = MagicMock()
        broker._started = True

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(return_value=MagicMock())

        with patch("rabbitkit.rpc.AsyncRPCClient", return_value=mock_client):
            await broker.request("test.queue", b"body")

        mock_client.call.assert_called_once_with(
            "test.queue", b"body", timeout=5.0, exchange="", headers=None
        )

    @pytest.mark.asyncio
    async def test_request_reuses_client(self) -> None:
        broker = AsyncBroker(RabbitConfig())
        broker._transport = MagicMock()
        broker._started = True

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(return_value=MagicMock())

        with patch("rabbitkit.rpc.AsyncRPCClient", return_value=mock_client) as mock_cls:
            await broker.request("q1", b"body1")
            await broker.request("q2", b"body2")

        # Constructor called once (reused)
        assert mock_cls.call_count == 1
        assert mock_client.call.call_count == 2

    @pytest.mark.asyncio
    async def test_request_passes_custom_params(self) -> None:
        broker = AsyncBroker(RabbitConfig())
        broker._transport = MagicMock()
        broker._started = True

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(return_value=MagicMock())

        with patch("rabbitkit.rpc.AsyncRPCClient", return_value=mock_client):
            await broker.request(
                "test.queue", b"body",
                timeout=10.0, exchange="my-exchange",
                headers={"x-custom": "value"},
            )

        mock_client.call.assert_called_once_with(
            "test.queue", b"body",
            timeout=10.0, exchange="my-exchange",
            headers={"x-custom": "value"},
        )

    @pytest.mark.asyncio
    async def test_stop_closes_rpc_client(self) -> None:
        broker = AsyncBroker(RabbitConfig())
        broker._transport = AsyncMock()
        broker._started = True

        mock_client = AsyncMock()
        mock_client.call = AsyncMock(return_value=MagicMock())
        mock_client.close = AsyncMock()

        with patch("rabbitkit.rpc.AsyncRPCClient", return_value=mock_client):
            await broker.request("q", b"body")

        await broker.stop()
        mock_client.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_without_request_no_error(self) -> None:
        broker = AsyncBroker(RabbitConfig())
        broker._transport = AsyncMock()
        broker._started = True
        # stop without ever calling request() should not error
        await broker.stop()
