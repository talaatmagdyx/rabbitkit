"""Tests for management.py — ManagementConfig and RabbitManagementClient."""

from __future__ import annotations

import base64
import json
import urllib.error
from contextlib import contextmanager
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

    def test_repr_masks_password(self) -> None:
        """L2: repr() must not leak the plaintext password."""
        config = ManagementConfig(username="admin", password="s3cr3t-p4ssw0rd")
        r = repr(config)
        assert "s3cr3t-p4ssw0rd" not in r
        assert "'***'" in r
        assert "admin" in r  # non-secret fields still shown


class TestManagementConfigGuestWarning:
    def test_guest_credentials_warn_for_non_local_host(self) -> None:
        """M-1: guest/guest against a non-local host emits one UserWarning."""
        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            ManagementConfig(url="http://rabbit.prod:15672")
        guest_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(guest_warnings) == 1
        assert "guest" in str(guest_warnings[0].message)

    def test_guest_credentials_no_warn_for_localhost(self) -> None:
        """M-1: guest/guest against localhost/127.0.0.1/::1 does NOT warn."""
        import warnings

        for url in ("http://localhost:15672", "http://127.0.0.1:15672", "http://[::1]:15672"):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                ManagementConfig(url=url)
            assert not [w for w in caught if issubclass(w.category, UserWarning)], (
                f"unexpected guest warning for url={url!r}"
            )

    def test_non_guest_credentials_no_warn(self) -> None:
        """M-1: non-guest credentials against a non-local host do NOT warn."""
        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            ManagementConfig(url="http://rabbit.prod:15672", username="admin", password="secret")
        assert not [w for w in caught if issubclass(w.category, UserWarning)]


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
    """Create a mock response object compatible with the client's chunked read.

    ``read`` streams the body once (in one chunk) then returns empty bytes on
    subsequent calls, matching the chunked-read contract used by the client.
    """
    mock_response = MagicMock()
    mock_response.status = status
    mock_response.__enter__ = MagicMock(return_value=mock_response)
    mock_response.__exit__ = MagicMock(return_value=False)

    if response_data is not None:
        body = json.dumps(response_data).encode()
        mock_response.read.side_effect = [body, b""]
    else:
        mock_response.read.side_effect = [b"", b""]

    return mock_response


@contextmanager
def _patch_open(client: RabbitManagementClient, response: MagicMock):
    """Patch the client's no-redirect opener so ``opener.open(...)`` returns ``response``.

    The client uses ``self._opener.open(req, timeout=...)`` instead of the
    module-level ``urllib.request.urlopen`` (so 3xx raise HTTPError, not follow).
    """
    with patch.object(client, "_opener") as mock_opener:
        mock_opener.open.return_value = response
        yield mock_opener


# ── Sync operation tests ─────────────────────────────────────────────────


class TestSyncOperations:
    def test_list_queues(self) -> None:
        queues = [{"name": "q1"}, {"name": "q2"}]
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(queues)) as mock_opener:
            result = client.list_queues()
        assert result == queues
        req = mock_opener.open.call_args[0][0]
        assert "/api/queues/%2F" in req.full_url

    def test_list_queues_custom_vhost(self) -> None:
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen([])) as mock_opener:
            client.list_queues(vhost="my-vhost")
        req = mock_opener.open.call_args[0][0]
        assert "/api/queues/my-vhost" in req.full_url

    def test_get_queue(self) -> None:
        queue_info = {"name": "test-queue", "messages": 10}
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(queue_info)) as mock_opener:
            result = client.get_queue("test-queue")
        assert result == queue_info
        req = mock_opener.open.call_args[0][0]
        assert "/api/queues/%2F/test-queue" in req.full_url

    def test_purge_queue(self) -> None:
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(status=204)) as mock_opener:
            client.purge_queue("test-queue")
        req = mock_opener.open.call_args[0][0]
        assert req.method == "DELETE"
        assert "/contents" in req.full_url

    def test_delete_queue(self) -> None:
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(status=204)) as mock_opener:
            client.delete_queue("test-queue")
        req = mock_opener.open.call_args[0][0]
        assert req.method == "DELETE"
        assert "/api/queues/%2F/test-queue" in req.full_url
        assert "/contents" not in req.full_url

    def test_list_exchanges(self) -> None:
        exchanges = [{"name": "amq.direct"}]
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(exchanges)):
            result = client.list_exchanges()
        assert result == exchanges

    def test_list_connections(self) -> None:
        conns = [{"name": "conn1"}]
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(conns)):
            result = client.list_connections()
        assert result == conns

    def test_list_channels(self) -> None:
        channels = [{"name": "ch1"}]
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(channels)):
            result = client.list_channels()
        assert result == channels

    def test_overview(self) -> None:
        overview_data = {"management_version": "3.12.0"}
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(overview_data)):
            result = client.overview()
        assert result == overview_data

    def test_health_check_ok(self) -> None:
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen({"status": "ok"})):
            assert client.health_check() is True

    def test_health_check_not_ok(self) -> None:
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen({"status": "failed"})):
            assert client.health_check() is False

    def test_health_check_exception(self) -> None:
        client = RabbitManagementClient()
        with patch.object(client, "_opener") as mock_opener:
            mock_opener.open.side_effect = ConnectionError("refused")
            assert client.health_check() is False

    def test_request_sets_headers(self) -> None:
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen({"ok": True})) as mock_opener:
            client.overview()
        req = mock_opener.open.call_args[0][0]
        assert req.get_header("Authorization").startswith("Basic ")
        assert req.get_header("Content-type") == "application/json"

    def test_204_returns_none(self) -> None:
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(status=204)):
            result = client.purge_queue("q")
        assert result is None

    def test_timeout_passed_to_opener(self) -> None:
        client = RabbitManagementClient(ManagementConfig(timeout=7.5))
        with _patch_open(client, _mock_urlopen({"ok": True})) as mock_opener:
            client.overview()
        assert mock_opener.open.call_args.kwargs["timeout"] == 7.5


# ── Default config ──────────────────────────────────────────────────────


class TestDefaultConfig:
    def test_default_config_when_none(self) -> None:
        client = RabbitManagementClient()
        assert client._config.url == "http://localhost:15672"
        assert client._config.username == "guest"


# ── Async helper ─────────────────────────────────────────────────────────


def _make_async_mock_session(
    response_data: dict | list | None = None,
    status: int = 200,
    *,
    body_chunks: list[bytes] | None = None,
    headers: dict[str, str] | None = None,
):
    """Build a mocked long-lived aiohttp.ClientSession (L-6).

    The client now reuses a single session and calls ``session.request(...)``
    (used as ``async with session.request(...) as resp:``). The mock streams
    the JSON body in chunks via ``resp.content.read(_READ_CHUNK)`` in a loop
    and then ``json.loads`` it. The response mock is exposed as
    ``session.mock_resp`` for tests that need to configure it further
    (e.g. raise_for_status). ``session.close`` is an awaitable noop.
    """
    mock_resp = AsyncMock()
    mock_resp.status = status
    mock_resp.headers = headers or {}
    mock_resp.raise_for_status = MagicMock()

    if body_chunks is not None:
        chunks = body_chunks
    elif response_data is not None:
        chunks = [json.dumps(response_data).encode()]
    else:
        chunks = [b"{}"]
    # Streaming read: return each chunk then an empty sentinel to signal EOF.
    mock_resp.content = MagicMock()
    mock_resp.content.read = AsyncMock(side_effect=[*chunks, b""])

    # session.request(...) is used as: async with session.request(...) as resp:
    mock_req_cm = AsyncMock()
    mock_req_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_req_cm.__aexit__ = AsyncMock(return_value=None)

    # aiohttp.ClientSession() is called once and the returned session is reused
    # across requests; the mock session is a plain object with .request/.close.
    mock_session = MagicMock()
    mock_session.request = MagicMock(return_value=mock_req_cm)
    mock_session.close = AsyncMock(return_value=None)
    # Expose the response mock for tests that need to configure it further.
    mock_session.mock_resp = mock_resp

    return mock_session


# ── Async operation tests ────────────────────────────────────────────────


class TestAsyncOperations:
    async def test_list_queues_async(self) -> None:
        queues = [{"name": "q1"}, {"name": "q2"}]
        mock_session_cm = _make_async_mock_session(queues)
        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client.list_queues_async()
        assert result == queues

    async def test_list_queues_async_custom_vhost(self) -> None:
        mock_session_cm = _make_async_mock_session([])
        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client.list_queues_async(vhost="my-vhost")
        assert result == []

    async def test_get_queue_async(self) -> None:
        queue_info = {"name": "test-queue", "messages": 5}
        mock_session_cm = _make_async_mock_session(queue_info)
        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client.get_queue_async("test-queue")
        assert result == queue_info

    async def test_overview_async(self) -> None:
        overview = {"management_version": "3.12.0"}
        mock_session_cm = _make_async_mock_session(overview)
        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client.overview_async()
        assert result == overview

    async def test_health_check_async_returns_true(self) -> None:
        mock_session_cm = _make_async_mock_session({"status": "ok"})
        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client.health_check_async()
        assert result is True

    async def test_health_check_async_returns_false_bad_status(self) -> None:
        mock_session_cm = _make_async_mock_session({"status": "failed"})
        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client.health_check_async()
        assert result is False

    async def test_health_check_async_returns_false_on_exception(self) -> None:
        client = RabbitManagementClient()
        with patch.object(client, "_request_async", side_effect=ConnectionError("refused")):
            result = await client.health_check_async()
        assert result is False

    async def test_request_async_204_returns_none(self) -> None:
        mock_session_cm = _make_async_mock_session(status=204, body_chunks=[b""])
        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client._request_async("DELETE", "/queues/%2F/test-queue")
        assert result is None

    async def test_request_async_raises_on_missing_aiohttp(self) -> None:
        client = RabbitManagementClient()
        with patch.dict("sys.modules", {"aiohttp": None}):
            with pytest.raises(ImportError, match="aiohttp"):
                await client._request_async("GET", "/overview")

    async def test_request_async_passes_allow_redirects_false(self) -> None:
        """The async client must disable redirect following (SSRF guard)."""
        mock_session = _make_async_mock_session({"ok": True})
        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session):
            await client.overview_async()
        assert mock_session.request.call_args.kwargs["allow_redirects"] is False


# ── L-6: long-lived aiohttp session reuse ─────────────────────────────────


def _make_req_cm(mock_resp):
    mock_req_cm = AsyncMock()
    mock_req_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_req_cm.__aexit__ = AsyncMock(return_value=None)
    return mock_req_cm


class TestAsyncSessionReuse:
    async def test_two_async_requests_reuse_one_session(self) -> None:
        """L-6: a single aiohttp.ClientSession is created and reused across calls."""
        resp1 = _make_async_mock_session({"ok": True}).mock_resp
        resp2 = _make_async_mock_session({"status": "ok"}).mock_resp
        mock_session = MagicMock()
        mock_session.request = MagicMock(side_effect=[_make_req_cm(resp1), _make_req_cm(resp2)])
        mock_session.close = AsyncMock(return_value=None)
        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session) as mock_ctor:
            await client.overview_async()
            await client.health_check_async()
        # ClientSession() constructor called exactly once across two requests.
        assert mock_ctor.call_count == 1
        # Both requests went through the same session object.
        assert mock_session.request.call_count == 2

    async def test_aclose_closes_session(self) -> None:
        """L-6: aclose() closes the lazily-created session and clears the handle."""
        mock_session = _make_async_mock_session({"ok": True})
        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session):
            await client.overview_async()
        assert client._aiohttp_session is mock_session
        await client.aclose()
        mock_session.close.assert_awaited_once()
        assert client._aiohttp_session is None

    async def test_aclose_is_noop_when_no_session(self) -> None:
        """L-6: aclose() is a no-op when no async request was made."""
        client = RabbitManagementClient()
        await client.aclose()
        assert client._aiohttp_session is None


# ── scheme validation + size cap ─────────────────────────────────────────


class TestManagementSchemeValidation:
    def test_https_scheme_accepted(self) -> None:
        config = ManagementConfig(url="https://rabbit.example:15672", username="admin", password="secret")
        assert config.url.startswith("https://")

    def test_invalid_scheme_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unsupported management URL scheme"):
            ManagementConfig(url="file:///etc/passwd")

    def test_ftp_scheme_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unsupported management URL scheme"):
            ManagementConfig(url="ftp://rabbit.example")

    def test_relative_url_rejected(self) -> None:
        with pytest.raises(ValueError, match="Unsupported management URL scheme"):
            ManagementConfig(url="//rabbit.example")

    def test_default_http_allowed_for_local_dev(self) -> None:
        # The default should still construct (http + guest/guest) for local dev.
        config = ManagementConfig()
        assert config.url == "http://localhost:15672"


class TestManagementResponseSizeCap:
    def test_oversized_response_raises(self) -> None:
        """A response exceeding the size cap raises ValueError."""
        from rabbitkit import management as mgmt

        mock_response = MagicMock()
        mock_response.status = 200
        mock_response.__enter__ = MagicMock(return_value=mock_response)
        mock_response.__exit__ = MagicMock(return_value=False)

        # Stream many 64KB chunks until the 64MB cap is exceeded.
        chunk = b"x" * mgmt._READ_CHUNK
        mock_response.read.side_effect = [chunk] * 1100 + [b""]

        client = RabbitManagementClient()
        with _patch_open(client, mock_response):
            with pytest.raises(ValueError, match="exceeded"):
                client.overview()

    def test_within_cap_response_read(self) -> None:
        """A normal-sized response is read fully."""
        data = {"name": "queue-1"}
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(data)):
            result = client.overview()
        assert result == data


# ── SSRF redirect mitigation (I-3) ───────────────────────────────────────


class TestSSRFRedirectMitigation:
    def test_sync_redirect_not_followed(self) -> None:
        """A 302 response is NOT followed — it surfaces as a ValueError (not a
        silent fetch of the redirect target, which could be an internal host)."""
        client = RabbitManagementClient()
        err = urllib.error.HTTPError(
            "http://rabbit/api/overview",
            302,
            "Found",
            {"Location": "http://169.254.169.254/latest/meta-data/"},
            __import__("io").BytesIO(b""),
        )
        with patch.object(client, "_opener") as mock_opener:
            mock_opener.open.side_effect = err
            with pytest.raises(ValueError, match="Unexpected redirect"):
                client.overview()

    def test_sync_redirect_health_check_returns_false(self) -> None:
        """health_check swallows the redirect error and reports unhealthy."""
        client = RabbitManagementClient()
        err = urllib.error.HTTPError(
            "http://rabbit/api/healthchecks/node",
            302,
            "Found",
            {"Location": "http://internal/"},
            __import__("io").BytesIO(b""),
        )
        with patch.object(client, "_opener") as mock_opener:
            mock_opener.open.side_effect = err
            assert client.health_check() is False

    async def test_async_redirect_not_followed(self) -> None:
        """The async path raises on a 3xx instead of reading the redirect target."""
        mock_session_cm = _make_async_mock_session(
            status=302,
            body_chunks=[b"<html>redirect</html>"],
            headers={"Location": "http://169.254.169.254/latest/meta-data/"},
        )
        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            with pytest.raises(ValueError, match="Unexpected redirect"):
                await client.overview_async()

    async def test_async_oversized_response_raises(self) -> None:
        """The async response cap mirrors the sync one."""
        from rabbitkit import management as mgmt

        chunk = b"x" * mgmt._READ_CHUNK
        mock_session_cm = _make_async_mock_session(status=200, body_chunks=[chunk] * 1100)
        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            with pytest.raises(ValueError, match="exceeded"):
                await client.overview_async()

    async def test_async_non_2xx_raises(self) -> None:
        """A non-2xx (non-3xx) response raises via raise_for_status."""
        mock_session_cm = _make_async_mock_session(status=500, body_chunks=[b"{}"])
        mock_session_cm.mock_resp.raise_for_status = MagicMock(side_effect=RuntimeError("server error"))
        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            with pytest.raises(RuntimeError, match="server error"):
                await client.overview_async()


# ── R7: QueueInfo TypedDict ─────────────────────────────────────────────


class TestQueueInfoTypedDict:
    """R7: list_queues / get_queue return typed QueueInfo dicts."""

    def test_list_queues_returns_queue_info(self) -> None:
        queues: list[dict[str, object]] = [
            {"name": "q1", "messages": 10, "consumers": 2},
            {"name": "q2", "durable": True, "messages_ready": 5},
        ]
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(queues)):
            result = client.list_queues()
        assert isinstance(result, list)
        assert result == queues
        # The return type is list[QueueInfo] — QueueInfo is a TypedDict (a dict
        # subclass at runtime), so items are plain dicts.
        assert result[0]["name"] == "q1"
        assert result[1]["durable"] is True

    def test_get_queue_returns_queue_info(self) -> None:
        queue_info: dict[str, object] = {
            "name": "orders",
            "vhost": "/",
            "type": "classic",
            "durable": True,
            "auto_delete": False,
            "messages": 42,
            "messages_ready": 40,
            "messages_unacknowledged": 2,
            "consumers": 3,
            "state": "running",
        }
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(queue_info)):
            result = client.get_queue("orders")
        assert result["name"] == "orders"
        assert result["messages"] == 42
        assert result["consumers"] == 3
        assert result["state"] == "running"

    @pytest.mark.asyncio
    async def test_list_queues_async_returns_queue_info(self) -> None:
        mock_session_cm = _make_async_mock_session(
            status=200, body_chunks=[json.dumps([{"name": "q1", "messages": 0}]).encode()]
        )
        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client.list_queues_async()
        assert result[0]["name"] == "q1"
        assert result[0]["messages"] == 0

    def test_queue_info_is_total_false(self) -> None:
        from rabbitkit.management import QueueInfo

        # TypedDict(total=False) — all keys optional.
        qi: QueueInfo = {}
        assert qi == {}

    def test_queue_info_all_fields(self) -> None:
        from rabbitkit.management import QueueInfo

        qi: QueueInfo = {
            "name": "q",
            "vhost": "/",
            "type": "classic",
            "durable": True,
            "auto_delete": False,
            "messages": 1,
            "messages_ready": 1,
            "messages_unacknowledged": 0,
            "consumers": 1,
            "state": "running",
        }
        assert qi["name"] == "q"
        assert qi["messages"] == 1


# ── R-TypedDict: typed dicts for more management API responses ───────────


class TestExchangeInfoTypedDict:
    def test_list_exchanges_returns_exchange_info(self) -> None:
        from rabbitkit.management import ExchangeInfo

        exchanges = [{"name": "amq.direct", "type": "direct", "durable": True}]
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(exchanges)):
            result = client.list_exchanges()
        assert isinstance(result, list)
        assert result == exchanges
        assert result[0]["name"] == "amq.direct"
        assert result[0]["type"] == "direct"
        # ExchangeInfo is a TypedDict (dict subclass at runtime).
        assert isinstance(result[0], dict)
        # TypedDict is structural — the returned dict satisfies ExchangeInfo.
        _ei: ExchangeInfo = result[0]
        assert _ei["name"] == "amq.direct"

    def test_list_exchanges_custom_vhost(self) -> None:
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen([])) as mock_opener:
            client.list_exchanges(vhost="my-vhost")
        req = mock_opener.open.call_args[0][0]
        assert "/api/exchanges/my-vhost" in req.full_url

    def test_get_exchange_returns_exchange_info(self) -> None:
        from rabbitkit.management import ExchangeInfo

        exchange = {"name": "orders", "type": "topic", "durable": True, "auto_delete": False}
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(exchange)) as mock_opener:
            result = client.get_exchange("orders")
        assert result == exchange
        assert result["type"] == "topic"
        req = mock_opener.open.call_args[0][0]
        assert "/api/exchanges/%2F/orders" in req.full_url
        _ei: ExchangeInfo = result
        assert _ei["name"] == "orders"

    def test_get_exchange_custom_vhost(self) -> None:
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen({"name": "x"})) as mock_opener:
            client.get_exchange("x", vhost="prod")
        req = mock_opener.open.call_args[0][0]
        assert "/api/exchanges/prod/x" in req.full_url

    def test_exchange_info_is_total_false(self) -> None:
        from rabbitkit.management import ExchangeInfo

        ei: ExchangeInfo = {}
        assert ei == {}

    def test_exchange_info_all_fields(self) -> None:
        from rabbitkit.management import ExchangeInfo

        ei: ExchangeInfo = {
            "name": "orders",
            "type": "topic",
            "durable": True,
            "auto_delete": False,
            "internal": False,
            "vhost": "/",
            "arguments": {"x-delayed-type": "direct"},
        }
        assert ei["name"] == "orders"
        assert ei["type"] == "topic"
        assert ei["durable"] is True
        assert ei["arguments"] == {"x-delayed-type": "direct"}


class TestConnectionInfoTypedDict:
    def test_list_connections_returns_connection_info(self) -> None:
        from rabbitkit.management import ConnectionInfo

        conns = [{"name": "conn1", "user": "guest", "channels": 3, "state": "running"}]
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(conns)):
            result = client.list_connections()
        assert isinstance(result, list)
        assert result == conns
        assert result[0]["channels"] == 3
        _ci: ConnectionInfo = result[0]
        assert _ci["user"] == "guest"

    def test_connection_info_is_total_false(self) -> None:
        from rabbitkit.management import ConnectionInfo

        ci: ConnectionInfo = {}
        assert ci == {}

    def test_connection_info_all_fields(self) -> None:
        from rabbitkit.management import ConnectionInfo

        ci: ConnectionInfo = {
            "name": "conn1",
            "vhost": "/",
            "user": "admin",
            "protocol": "AMQP 0-9-1",
            "state": "running",
            "channels": 5,
            "peer_host": "10.0.0.1",
            "peer_port": 5672,
        }
        assert ci["channels"] == 5
        assert ci["peer_port"] == 5672


class TestChannelInfoTypedDict:
    def test_list_channels_returns_channel_info(self) -> None:
        from rabbitkit.management import ChannelInfo

        channels = [{"number": 1, "user": "guest", "vhost": "/", "state": "running"}]
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(channels)):
            result = client.list_channels()
        assert isinstance(result, list)
        assert result == channels
        assert result[0]["number"] == 1
        _chi: ChannelInfo = result[0]
        assert _chi["user"] == "guest"

    def test_channel_info_is_total_false(self) -> None:
        from rabbitkit.management import ChannelInfo

        chi: ChannelInfo = {}
        assert chi == {}

    def test_channel_info_all_fields(self) -> None:
        from rabbitkit.management import ChannelInfo

        chi: ChannelInfo = {
            "number": 7,
            "user": "admin",
            "vhost": "/",
            "connection_name": "conn-1",
            "state": "running",
        }
        assert chi["number"] == 7
        assert chi["connection_name"] == "conn-1"


class TestOverviewInfoTypedDict:
    def test_overview_returns_overview_info(self) -> None:
        from rabbitkit.management import OverviewInfo

        overview_data = {
            "rabbitmq_version": "3.12.0",
            "erlang_version": "25.3",
            "cluster_name": "rabbit@node1",
            "contexts": [{"path": "/", "port": 15672}],
            "listeners": [{"node": "rabbit@node1", "protocol": "amqp"}],
        }
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(overview_data)):
            result = client.overview()
        assert result == overview_data
        assert result["rabbitmq_version"] == "3.12.0"
        _oi: OverviewInfo = result
        assert _oi["cluster_name"] == "rabbit@node1"
        assert _oi["listeners"][0]["protocol"] == "amqp"

    @pytest.mark.asyncio
    async def test_overview_async_returns_overview_info(self) -> None:
        from rabbitkit.management import OverviewInfo

        overview = {"rabbitmq_version": "3.12.0", "cluster_name": "rabbit@node1"}
        mock_session_cm = _make_async_mock_session(overview)
        client = RabbitManagementClient()
        with patch("aiohttp.ClientSession", return_value=mock_session_cm):
            result = await client.overview_async()
        assert result["rabbitmq_version"] == "3.12.0"
        _oi: OverviewInfo = result
        assert _oi["cluster_name"] == "rabbit@node1"

    def test_overview_info_is_total_false(self) -> None:
        from rabbitkit.management import OverviewInfo

        oi: OverviewInfo = {}
        assert oi == {}

    def test_overview_info_all_fields(self) -> None:
        from rabbitkit.management import OverviewInfo

        oi: OverviewInfo = {
            "rabbitmq_version": "3.13.0",
            "erlang_version": "26.0",
            "cluster_name": "rabbit@node1",
            "contexts": [{"path": "/"}],
            "listeners": [{"protocol": "amqp"}],
        }
        assert oi["rabbitmq_version"] == "3.13.0"
        assert oi["contexts"][0]["path"] == "/"


# ── Uncovered-line coverage additions ────────────────────────────────────────


class TestNoRedirectHandler:
    """Line 174: _NoRedirect.redirect_request returns None (rejects all redirects)."""

    def test_redirect_request_returns_none(self) -> None:
        from rabbitkit.management import _NoRedirect

        handler = _NoRedirect()
        result = handler.redirect_request()
        assert result is None

    def test_redirect_request_ignores_all_args(self) -> None:
        from rabbitkit.management import _NoRedirect

        handler = _NoRedirect()
        # Should return None regardless of what arguments are passed.
        result = handler.redirect_request("arg1", "arg2", key="value")
        assert result is None


class TestSyncRequestNonRedirectHTTPError:
    """Line 234: bare ``raise`` inside ``_request`` re-raises a non-redirect
    ``HTTPError`` (e.g. 404, 500) — the 3xx-guard condition is False so the
    ``raise ValueError`` branch is skipped and the original exception propagates.
    """

    def test_non_redirect_http_error_is_reraised(self) -> None:
        """A 500 HTTPError must propagate unchanged (line 234: ``raise``)."""
        client = RabbitManagementClient()
        err = urllib.error.HTTPError(
            "http://localhost:15672/api/overview",
            500,
            "Internal Server Error",
            {},
            __import__("io").BytesIO(b""),
        )
        with patch.object(client, "_opener") as mock_opener:
            mock_opener.open.side_effect = err
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                client._request("GET", "/overview")
        assert exc_info.value.code == 500

    def test_404_http_error_is_reraised(self) -> None:
        """A 404 HTTPError (not a redirect) must also propagate (line 234)."""
        client = RabbitManagementClient()
        err = urllib.error.HTTPError(
            "http://localhost:15672/api/queues/%2F/missing",
            404,
            "Not Found",
            {},
            __import__("io").BytesIO(b""),
        )
        with patch.object(client, "_opener") as mock_opener:
            mock_opener.open.side_effect = err
            with pytest.raises(urllib.error.HTTPError) as exc_info:
                client._request("GET", "/queues/%2F/missing")
        assert exc_info.value.code == 404


class TestGetSessionImportError:
    """Lines 341-342: ``_get_session()`` raises ``ImportError`` (with a helpful
    message) when ``aiohttp`` is not installed.

    This mirrors ``test_request_async_raises_on_missing_aiohttp`` in
    ``TestAsyncOperations`` but targets ``_get_session`` directly.
    """

    @pytest.mark.asyncio
    async def test_get_session_raises_import_error_when_aiohttp_missing(self) -> None:
        """Lines 341-342: ``except ImportError: raise ImportError(...)`` fires
        when the ``import aiohttp`` inside ``_get_session`` fails."""
        client = RabbitManagementClient()
        with patch.dict("sys.modules", {"aiohttp": None}):
            with pytest.raises(ImportError, match="aiohttp"):
                await client._get_session()


# ── Migration helpers: parameters, shovels, bindings, queue declaration ──


class TestParameterOperations:
    def test_put_parameter(self) -> None:
        """PUT /api/parameters/{component}/{vhost}/{name} with a JSON body."""
        client = RabbitManagementClient()
        value = {"value": {"src-queue": "a", "dest-queue": "b"}}
        with _patch_open(client, _mock_urlopen(status=201)) as mock_opener:
            result = client.put_parameter("shovel", "/", "move-a-b", value)
        assert result is None
        req = mock_opener.open.call_args[0][0]
        assert req.method == "PUT"
        assert "/api/parameters/shovel/%2F/move-a-b" in req.full_url
        assert json.loads(req.data.decode()) == value

    def test_put_parameter_encodes_path_segments(self) -> None:
        """Component, vhost, and name are URL-encoded like other methods."""
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(status=201)) as mock_opener:
            client.put_parameter("shovel", "my/vhost", "a b", {"value": {}})
        req = mock_opener.open.call_args[0][0]
        assert "/api/parameters/shovel/my%2Fvhost/a%20b" in req.full_url

    def test_delete_parameter(self) -> None:
        """DELETE /api/parameters/{component}/{vhost}/{name}."""
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(status=204)) as mock_opener:
            result = client.delete_parameter("shovel", "/", "move-a-b")
        assert result is None
        req = mock_opener.open.call_args[0][0]
        assert req.method == "DELETE"
        assert "/api/parameters/shovel/%2F/move-a-b" in req.full_url


class TestShovelStatuses:
    def test_list_shovel_statuses(self) -> None:
        """GET /api/shovels returns the raw status list."""
        statuses = [{"name": "move-a-b", "state": "running"}]
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(statuses)) as mock_opener:
            result = client.list_shovel_statuses()
        assert result == statuses
        req = mock_opener.open.call_args[0][0]
        assert req.method == "GET"
        assert req.full_url.endswith("/api/shovels")

    def test_list_shovel_statuses_raises_when_plugin_missing(self) -> None:
        """A 404 (shovel plugin disabled) propagates to the caller."""
        client = RabbitManagementClient()
        err = urllib.error.HTTPError(
            "http://localhost:15672/api/shovels",
            404,
            "Not Found",
            {},
            __import__("io").BytesIO(b""),
        )
        with patch.object(client, "_opener") as mock_opener:
            mock_opener.open.side_effect = err
            with pytest.raises(urllib.error.HTTPError):
                client.list_shovel_statuses()


class TestQueueBindingOperations:
    def test_get_queue_bindings(self) -> None:
        """GET /api/queues/{vhost}/{queue}/bindings returns the binding list."""
        bindings = [{"source": "ex", "destination": "orders.q", "routing_key": "rk"}]
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(bindings)) as mock_opener:
            result = client.get_queue_bindings("orders.q")
        assert result == bindings
        req = mock_opener.open.call_args[0][0]
        assert req.method == "GET"
        assert "/api/queues/%2F/orders.q/bindings" in req.full_url

    def test_get_queue_bindings_encodes_segments(self) -> None:
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen([])) as mock_opener:
            client.get_queue_bindings("a/b", vhost="vh")
        req = mock_opener.open.call_args[0][0]
        assert "/api/queues/vh/a%2Fb/bindings" in req.full_url

    def test_declare_queue(self) -> None:
        """PUT /api/queues/{vhost}/{name} with durable/auto_delete/arguments body."""
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(status=201)) as mock_opener:
            result = client.declare_queue("new.q", arguments={"x-queue-type": "quorum"})
        assert result is None
        req = mock_opener.open.call_args[0][0]
        assert req.method == "PUT"
        assert "/api/queues/%2F/new.q" in req.full_url
        assert json.loads(req.data.decode()) == {
            "durable": True,
            "auto_delete": False,
            "arguments": {"x-queue-type": "quorum"},
        }

    def test_declare_queue_default_arguments(self) -> None:
        """arguments=None serializes as an empty dict."""
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(status=201)) as mock_opener:
            client.declare_queue("plain.q", vhost="vh", durable=False)
        req = mock_opener.open.call_args[0][0]
        assert "/api/queues/vh/plain.q" in req.full_url
        assert json.loads(req.data.decode()) == {"durable": False, "auto_delete": False, "arguments": {}}

    def test_bind_queue(self) -> None:
        """POST /api/bindings/{vhost}/e/{exchange}/q/{queue} with routing_key body."""
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(status=201)) as mock_opener:
            result = client.bind_queue("my.q", "my-ex", "orders.#", arguments={"x-match": "all"})
        assert result is None
        req = mock_opener.open.call_args[0][0]
        assert req.method == "POST"
        assert "/api/bindings/%2F/e/my-ex/q/my.q" in req.full_url
        assert json.loads(req.data.decode()) == {"routing_key": "orders.#", "arguments": {"x-match": "all"}}

    def test_bind_queue_default_arguments(self) -> None:
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(status=201)) as mock_opener:
            client.bind_queue("q/1", "e/1", vhost="vh")
        req = mock_opener.open.call_args[0][0]
        assert "/api/bindings/vh/e/e%2F1/q/q%2F1" in req.full_url
        assert json.loads(req.data.decode()) == {"routing_key": "", "arguments": {}}


class TestEmptyBodyResponses:
    def test_200_with_empty_body_returns_none(self) -> None:
        """A 2xx response with an empty body (e.g. 201 Created) returns None
        instead of crashing in json.loads."""
        client = RabbitManagementClient()
        with _patch_open(client, _mock_urlopen(status=200)):
            result = client._request("PUT", "/queues/%2F/q")
        assert result is None
