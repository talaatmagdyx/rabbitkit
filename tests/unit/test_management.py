"""Tests for management.py — ManagementConfig and RabbitManagementClient."""

from __future__ import annotations

import base64
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rabbitkit.management import ManagementConfig, RabbitManagementClient

# ── ManagementConfig tests ───────────────────────────────────────────────


class TestManagementConfig:
    def test_defaults(self) -> None:
        config = ManagementConfig()
        assert config.url == "http://localhost:15672"
        assert config.username == "guest"
        assert config.password == "guest"
        assert config.timeout == 10.0

    def test_custom_values(self) -> None:
        config = ManagementConfig(url="http://rabbit:15672", username="admin", password="secret", timeout=5.0)
        assert config.url == "http://rabbit:15672"
        assert config.username == "admin"
        assert config.password == "secret"
        assert config.timeout == 5.0

    def test_frozen(self) -> None:
        config = ManagementConfig()
        with pytest.raises(AttributeError):
            config.url = "http://other"  # type: ignore[misc]


# ── Auth header ──────────────────────────────────────────────────────────


class TestAuthHeader:
    def test_basic_auth_encoding(self) -> None:
        client = RabbitManagementClient()
        expected = "Basic " + base64.b64encode(b"guest:guest").decode()
        assert client._auth_header == expected

    def test_custom_credentials(self) -> None:
        config = ManagementConfig(username="admin", password="p@ss")
        client = RabbitManagementClient(config)
        expected = "Basic " + base64.b64encode(b"admin:p@ss").decode()
        assert client._auth_header == expected


# ── Sync request helpers ─────────────────────────────────────────────────


def _mock_urlopen(response_data: dict | list | None = None, status: int = 200):
    """Create a mock for urllib.request.urlopen."""
    mock_response = MagicMock()
    mock_response.status = status
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)

    if response_data is not None:
        mock_response.read.return_value = json.dumps(response_data).encode()
    else:
        mock_response.read.return_value = b""

    return mock_response


# ── Sync operation tests ─────────────────────────────────────────────────


class TestSyncOperations:
    @patch("rabbitkit.management.urllib.request.urlopen")
    def test_list_queues(self, mock_open: MagicMock) -> None:
        queues = [{"name": "q1"}, {"name": "q2"}]
        mock_open.return_value = _mock_urlopen(queues)

        client = RabbitManagementClient()
        result = client.list_queues()

        assert result == queues
        req = mock_open.call_args[0][0]
        assert "/api/queues/%2F" in req.full_url

    @patch("rabbitkit.management.urllib.request.urlopen")
    def test_list_queues_custom_vhost(self, mock_open: MagicMock) -> None:
        mock_open.return_value = _mock_urlopen([])

        client = RabbitManagementClient()
        client.list_queues(vhost="my-vhost")

        req = mock_open.call_args[0][0]
        assert "/api/queues/my-vhost" in req.full_url

    @patch("rabbitkit.management.urllib.request.urlopen")
    def test_get_queue(self, mock_open: MagicMock) -> None:
        queue_info = {"name": "test-queue", "messages": 10}
        mock_open.return_value = _mock_urlopen(queue_info)

        client = RabbitManagementClient()
        result = client.get_queue("test-queue")

        assert result == queue_info
        req = mock_open.call_args[0][0]
        assert "/api/queues/%2F/test-queue" in req.full_url

    @patch("rabbitkit.management.urllib.request.urlopen")
    def test_purge_queue(self, mock_open: MagicMock) -> None:
        mock_open.return_value = _mock_urlopen(status=204)

        client = RabbitManagementClient()
        client.purge_queue("test-queue")

        req = mock_open.call_args[0][0]
        assert req.method == "DELETE"
        assert "/contents" in req.full_url

    @patch("rabbitkit.management.urllib.request.urlopen")
    def test_delete_queue(self, mock_open: MagicMock) -> None:
        mock_open.return_value = _mock_urlopen(status=204)

        client = RabbitManagementClient()
        client.delete_queue("test-queue")

        req = mock_open.call_args[0][0]
        assert req.method == "DELETE"
        assert "/api/queues/%2F/test-queue" in req.full_url
        assert "/contents" not in req.full_url

    @patch("rabbitkit.management.urllib.request.urlopen")
    def test_list_exchanges(self, mock_open: MagicMock) -> None:
        exchanges = [{"name": "amq.direct"}]
        mock_open.return_value = _mock_urlopen(exchanges)

        client = RabbitManagementClient()
        result = client.list_exchanges()

        assert result == exchanges

    @patch("rabbitkit.management.urllib.request.urlopen")
    def test_list_connections(self, mock_open: MagicMock) -> None:
        conns = [{"name": "conn1"}]
        mock_open.return_value = _mock_urlopen(conns)

        client = RabbitManagementClient()
        result = client.list_connections()

        assert result == conns

    @patch("rabbitkit.management.urllib.request.urlopen")
    def test_list_channels(self, mock_open: MagicMock) -> None:
        channels = [{"name": "ch1"}]
        mock_open.return_value = _mock_urlopen(channels)

        client = RabbitManagementClient()
        result = client.list_channels()

        assert result == channels

    @patch("rabbitkit.management.urllib.request.urlopen")
    def test_overview(self, mock_open: MagicMock) -> None:
        overview_data = {"management_version": "3.12.0"}
        mock_open.return_value = _mock_urlopen(overview_data)

        client = RabbitManagementClient()
        result = client.overview()

        assert result == overview_data

    @patch("rabbitkit.management.urllib.request.urlopen")
    def test_health_check_ok(self, mock_open: MagicMock) -> None:
        mock_open.return_value = _mock_urlopen({"status": "ok"})

        client = RabbitManagementClient()
        assert client.health_check() is True

    @patch("rabbitkit.management.urllib.request.urlopen")
    def test_health_check_not_ok(self, mock_open: MagicMock) -> None:
        mock_open.return_value = _mock_urlopen({"status": "failed"})

        client = RabbitManagementClient()
        assert client.health_check() is False

    @patch("rabbitkit.management.urllib.request.urlopen")
    def test_health_check_exception(self, mock_open: MagicMock) -> None:
        mock_open.side_effect = ConnectionError("refused")

        client = RabbitManagementClient()
        assert client.health_check() is False

    @patch("rabbitkit.management.urllib.request.urlopen")
    def test_request_sets_headers(self, mock_open: MagicMock) -> None:
        mock_open.return_value = _mock_urlopen({"ok": True})

        client = RabbitManagementClient()
        client.overview()

        req = mock_open.call_args[0][0]
        assert req.get_header("Authorization").startswith("Basic ")
        assert req.get_header("Content-type") == "application/json"

    @patch("rabbitkit.management.urllib.request.urlopen")
    def test_204_returns_none(self, mock_open: MagicMock) -> None:
        mock_open.return_value = _mock_urlopen(status=204)

        client = RabbitManagementClient()
        result = client.purge_queue("q")

        assert result is None


# ── Default config ───────────────────────────────────────────────────────


class TestDefaultConfig:
    def test_default_config_when_none(self) -> None:
        client = RabbitManagementClient()
        assert client._config.url == "http://localhost:15672"
        assert client._config.username == "guest"


# ── Async helper ─────────────────────────────────────────────────────────


def _make_async_mock_session(response_data: dict | list | None = None, status: int = 200):
    """Build a fully mocked aiohttp.ClientSession context manager."""
    mock_resp = AsyncMock()
    mock_resp.status = status
    mock_resp.json = AsyncMock(return_value=response_data if response_data is not None else {})

    # session.request(...) is used as: async with session.request(...) as resp:
    mock_req_cm = AsyncMock()
    mock_req_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_req_cm.__aexit__ = AsyncMock(return_value=None)

    mock_session = MagicMock()
    mock_session.request = MagicMock(return_value=mock_req_cm)

    # aiohttp.ClientSession() is used as: async with aiohttp.ClientSession() as session:
    mock_session_cm = AsyncMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=None)

    return mock_session_cm


# ── Async operation tests (lines 155-193) ────────────────────────────────


class TestAsyncOperations:
    async def test_list_queues_async(self) -> None:
        """Lines 155-156: list_queues_async calls _request_async."""
        queues = [{"name": "q1"}, {"name": "q2"}]
        mock_session_cm = _make_async_mock_session(queues)

        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client.list_queues_async()

        assert result == queues

    async def test_list_queues_async_custom_vhost(self) -> None:
        """list_queues_async with a custom vhost encodes the URL correctly."""
        mock_session_cm = _make_async_mock_session([])

        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client.list_queues_async(vhost="my-vhost")

        assert result == []

    async def test_get_queue_async(self) -> None:
        """Lines 157-160: get_queue_async."""
        queue_info = {"name": "test-queue", "messages": 5}
        mock_session_cm = _make_async_mock_session(queue_info)

        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client.get_queue_async("test-queue")

        assert result == queue_info

    async def test_overview_async(self) -> None:
        """Lines 162-163: overview_async."""
        overview = {"management_version": "3.12.0"}
        mock_session_cm = _make_async_mock_session(overview)

        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client.overview_async()

        assert result == overview

    async def test_health_check_async_returns_true(self) -> None:
        """Lines 165-170: health_check_async returns True when status=ok."""
        mock_session_cm = _make_async_mock_session({"status": "ok"})

        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client.health_check_async()

        assert result is True

    async def test_health_check_async_returns_false_bad_status(self) -> None:
        """Lines 165-170: health_check_async returns False when status != ok."""
        mock_session_cm = _make_async_mock_session({"status": "failed"})

        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client.health_check_async()

        assert result is False

    async def test_health_check_async_returns_false_on_exception(self) -> None:
        """Lines 165-170: health_check_async returns False when exception raised."""
        client = RabbitManagementClient()
        with patch.object(client, "_request_async", side_effect=ConnectionError("refused")):
            result = await client.health_check_async()

        assert result is False

    async def test_request_async_204_returns_none(self) -> None:
        """Lines 191-192: _request_async returns None for 204 responses."""
        mock_session_cm = _make_async_mock_session(status=204)

        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client._request_async("DELETE", "/queues/%2F/test-queue")

        assert result is None

    async def test_request_async_raises_on_missing_aiohttp(self) -> None:
        """Lines 173-179: _request_async raises ImportError when aiohttp unavailable."""
        client = RabbitManagementClient()
        with patch.dict("sys.modules", {"aiohttp": None}):
            with pytest.raises(ImportError, match="aiohttp"):
                await client._request_async("GET", "/overview")
