"""Tests for SyncBroker.request() shorthand (F4)."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from rabbitkit.core.config import RabbitConfig
from rabbitkit.sync.broker import SyncBroker


class TestSyncBrokerRequest:

    def test_request_before_start_raises(self) -> None:
        broker = SyncBroker(RabbitConfig())
        with pytest.raises(RuntimeError, match="Broker not started"):
            broker.request("test.queue", b"body")

    def test_request_creates_rpc_client(self) -> None:
        broker = SyncBroker(RabbitConfig())
        broker._transport = MagicMock()
        broker._started = True

        mock_client = MagicMock()
        mock_client.call = MagicMock(return_value=MagicMock())

        with patch("rabbitkit.rpc.RPCClient", return_value=mock_client):
            broker.request("test.queue", b"body")

        mock_client.call.assert_called_once_with(
            "test.queue", b"body", timeout=5.0, exchange="", headers=None
        )

    def test_request_reuses_client(self) -> None:
        broker = SyncBroker(RabbitConfig())
        broker._transport = MagicMock()
        broker._started = True

        mock_client = MagicMock()
        mock_client.call = MagicMock(return_value=MagicMock())

        with patch("rabbitkit.rpc.RPCClient", return_value=mock_client) as mock_cls:
            broker.request("q1", b"body1")
            broker.request("q2", b"body2")

        assert mock_cls.call_count == 1
        assert mock_client.call.call_count == 2

    def test_stop_closes_rpc_client(self) -> None:
        broker = SyncBroker(RabbitConfig())
        broker._transport = MagicMock()
        broker._started = True

        mock_client = MagicMock()
        mock_client.call = MagicMock(return_value=MagicMock())

        with patch("rabbitkit.rpc.RPCClient", return_value=mock_client):
            broker.request("q", b"body")

        broker.stop()
        mock_client.close.assert_called_once()

    def test_stop_without_request_no_error(self) -> None:
        broker = SyncBroker(RabbitConfig())
        broker._transport = MagicMock()
        broker._started = True
        broker.stop()
