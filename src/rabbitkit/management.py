"""RabbitMQ Management HTTP API client.

Provides a thin Python wrapper around the **RabbitMQ Management HTTP API**
(available on port 15672 by default).

* **Sync** operations use ``urllib.request`` — no extra dependencies.
* **Async** operations use ``aiohttp`` — requires ``pip install rabbitkit[management]``.

Security
--------
* Schemes other than ``http``/``https`` are rejected at construction
  (prevents ``file://``/``gopher://`` SSRF).
* Redirects are **not** followed (sync and async) — a 3xx response is surfaced
  as an error so a crafted ``Location`` header cannot redirect the client to an
  internal host.
* Response bodies are capped (``_MAX_RESPONSE_BYTES``) to guard against
  runaway / zip-bomb responses.

Quick start
-----------
    from rabbitkit.management import RabbitManagementClient, ManagementConfig

    client = RabbitManagementClient(
        ManagementConfig(
            url="http://rabbitmq:15672",
            username="admin",
            password="secret",
        )
    )

    # List all queues in the default vhost
    for q in client.list_queues():
        print(q["name"], q["messages"])

    # Purge a queue
    client.purge_queue("orders", vhost="/production")

    # Check if the node is healthy
    if not client.health_check():
        alert("RabbitMQ node is unhealthy!")

Async usage (requires aiohttp)
-------------------------------
    async with AsyncBroker(...) as broker:
        client = RabbitManagementClient()
        queues = await client.list_queues_async()
        print(queues)

Available operations
--------------------
Sync:

    client.list_queues(vhost="/")          -> list[QueueInfo]
    client.get_queue(name, vhost="/")      -> QueueInfo
    client.purge_queue(name, vhost="/")    -> None
    client.delete_queue(name, vhost="/")   -> None
    client.list_exchanges(vhost="/")       -> list[ExchangeInfo]
    client.get_exchange(name, vhost="/")   -> ExchangeInfo
    client.list_connections()              -> list[ConnectionInfo]
    client.list_channels()                 -> list[ChannelInfo]
    client.overview()                      -> OverviewInfo
    client.health_check()                  -> bool

Async (all have ``_async`` suffix):

    await client.list_queues_async()
    await client.get_queue_async(name)
    await client.overview_async()
    await client.health_check_async()

Dashboard integration
---------------------
Pass a ``RabbitManagementClient`` instance to ``create_dashboard_app()`` to
enrich the monitoring dashboard with live queue stats:

    from rabbitkit.management import RabbitManagementClient
    from rabbitkit.dashboard import create_dashboard_app

    mgmt = RabbitManagementClient()
    app  = create_dashboard_app(broker, management_client=mgmt)
"""

from __future__ import annotations

import base64
import json
import urllib.error
import urllib.parse
import urllib.request
import warnings
from dataclasses import dataclass
from typing import Any, TypedDict, cast

# Hard cap on a single Management API response body (zip-bomb / runaway guard).
_MAX_RESPONSE_BYTES = 64 * 1024 * 1024
# Read in chunks of this size when enforcing the response cap.
_READ_CHUNK = 64 * 1024


class QueueInfo(TypedDict, total=False):
    """Typed view of a RabbitMQ Management API queue response.

    The Management API returns many fields; this captures the most common
    ones. ``total=False`` means every key is optional — the API may omit
    fields depending on the endpoint and RabbitMQ version.
    """

    name: str
    vhost: str
    type: str
    durable: bool
    auto_delete: bool
    messages: int
    messages_ready: int
    messages_unacknowledged: int
    consumers: int
    state: str


class ExchangeInfo(TypedDict, total=False):
    """Typed view of a RabbitMQ Management API exchange response."""

    name: str
    type: str
    durable: bool
    auto_delete: bool
    internal: bool
    vhost: str
    arguments: dict[str, Any]


class ConnectionInfo(TypedDict, total=False):
    """Typed view of a RabbitMQ Management API connection response."""

    name: str
    vhost: str
    user: str
    protocol: str
    state: str
    channels: int
    peer_host: str
    peer_port: int


class ChannelInfo(TypedDict, total=False):
    """Typed view of a RabbitMQ Management API channel response."""

    number: int
    user: str
    vhost: str
    connection_name: str
    state: str


class OverviewInfo(TypedDict, total=False):
    """Typed view of the RabbitMQ Management API ``/overview`` response."""

    rabbitmq_version: str
    erlang_version: str
    cluster_name: str
    contexts: list[dict[str, Any]]
    listeners: list[dict[str, Any]]


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    """Reject all redirects — return ``None`` so urllib raises ``HTTPError``.

    With this handler installed, a 3xx response surfaces as ``HTTPError`` (no
    handler claims it), which the caller catches and turns into a ``ValueError``
    — preventing silent SSRF via a ``Location`` header pointing at an internal host.
    """

    def redirect_request(self, *args: Any, **kwargs: Any) -> urllib.request.Request | None:
        return None


@dataclass(frozen=True, slots=True)
class ManagementConfig:
    """RabbitMQ Management API configuration.

    .. warning::
        The default URL uses ``http://`` with ``guest``/``guest`` credentials —
        this is intended **only for local development**. In production use
        ``https://`` with non-default credentials. Schemes other than
        ``http``/``https`` are rejected to prevent SSRF via crafted URLs.
    """

    url: str = "http://localhost:15672"
    username: str = "guest"
    password: str = "guest"
    timeout: float = 10.0

    def __post_init__(self) -> None:
        scheme = urllib.parse.urlparse(self.url).scheme.lower()
        if scheme not in {"http", "https"}:
            raise ValueError(f"Unsupported management URL scheme {scheme!r}; only 'http' and 'https' are allowed.")
        # Mirror ConnectionConfig.__post_init__: warn when the default 'guest'
        # credentials are used against a non-local host (dev convenience, but
        # flag the production misconfiguration once at construction).
        hostname = urllib.parse.urlparse(self.url).hostname
        if self.username == "guest" and hostname not in {"localhost", "127.0.0.1", "::1", None}:
            warnings.warn(
                "ManagementConfig uses default 'guest' credentials against non-local "
                f"host {hostname!r}; set explicit username/password for production.",
                UserWarning,
                stacklevel=2,
            )


class RabbitManagementClient:
    """HTTP client for the RabbitMQ Management API."""

    def __init__(self, config: ManagementConfig | None = None) -> None:
        self._config = config or ManagementConfig()
        credentials = f"{self._config.username}:{self._config.password}"
        self._auth_header = "Basic " + base64.b64encode(credentials.encode()).decode()
        # No-redirect opener: 3xx raise HTTPError instead of being followed.
        self._opener = urllib.request.build_opener(_NoRedirect)
        # Long-lived aiohttp session reused across *_async requests (L-6).
        # Lazily created on the first async request; closed via aclose().
        self._aiohttp_session: Any = None

    def _request(self, method: str, path: str, body: bytes | None = None) -> Any:
        url = f"{self._config.url}/api{path}"
        req = urllib.request.Request(url, method=method, data=body)  # noqa: S310
        req.add_header("Authorization", self._auth_header)
        req.add_header("Content-Type", "application/json")
        try:
            resp = self._opener.open(req, timeout=self._config.timeout)
        except urllib.error.HTTPError as exc:
            # With _NoRedirect installed, 3xx surface here as HTTPError.
            if 300 <= exc.code < 400:
                raise ValueError(f"Unexpected redirect ({exc.code}) to {exc.headers.get('Location')}") from exc
            raise
        with resp:
            if resp.status == 204:
                # Drain + discard any (usually empty) body.
                return None
            return json.loads(self._read_capped(resp).decode())

    def _read_capped(self, resp: Any) -> bytes:
        """Read the response body in chunks, raising if it exceeds the cap."""
        total = 0
        chunks: list[bytes] = []
        while True:
            chunk = resp.read(_READ_CHUNK)
            if not chunk:
                break
            total += len(chunk)
            if total > _MAX_RESPONSE_BYTES:
                raise ValueError(f"Management API response exceeded {_MAX_RESPONSE_BYTES} bytes")
            chunks.append(chunk)
        return b"".join(chunks)

    # Queue operations
    def list_queues(self, vhost: str = "/") -> list[QueueInfo]:
        vhost_encoded = urllib.parse.quote(vhost, safe="")
        return cast("list[QueueInfo]", self._request("GET", f"/queues/{vhost_encoded}"))

    def get_queue(self, name: str, vhost: str = "/") -> QueueInfo:
        vhost_encoded = urllib.parse.quote(vhost, safe="")
        name_encoded = urllib.parse.quote(name, safe="")
        return cast("QueueInfo", self._request("GET", f"/queues/{vhost_encoded}/{name_encoded}"))

    def purge_queue(self, name: str, vhost: str = "/") -> None:
        vhost_encoded = urllib.parse.quote(vhost, safe="")
        name_encoded = urllib.parse.quote(name, safe="")
        self._request("DELETE", f"/queues/{vhost_encoded}/{name_encoded}/contents")

    def delete_queue(self, name: str, vhost: str = "/") -> None:
        vhost_encoded = urllib.parse.quote(vhost, safe="")
        name_encoded = urllib.parse.quote(name, safe="")
        self._request("DELETE", f"/queues/{vhost_encoded}/{name_encoded}")

    # Exchange operations
    def list_exchanges(self, vhost: str = "/") -> list[ExchangeInfo]:
        vhost_encoded = urllib.parse.quote(vhost, safe="")
        return cast("list[ExchangeInfo]", self._request("GET", f"/exchanges/{vhost_encoded}"))

    def get_exchange(self, name: str, vhost: str = "/") -> ExchangeInfo:
        vhost_encoded = urllib.parse.quote(vhost, safe="")
        name_encoded = urllib.parse.quote(name, safe="")
        return cast("ExchangeInfo", self._request("GET", f"/exchanges/{vhost_encoded}/{name_encoded}"))

    # Connection/Channel
    def list_connections(self) -> list[ConnectionInfo]:
        return cast("list[ConnectionInfo]", self._request("GET", "/connections"))

    def list_channels(self) -> list[ChannelInfo]:
        return cast("list[ChannelInfo]", self._request("GET", "/channels"))

    # Overview
    def overview(self) -> OverviewInfo:
        return cast("OverviewInfo", self._request("GET", "/overview"))

    def health_check(self) -> bool:
        try:
            result = self._request("GET", "/healthchecks/node")
            return bool(result.get("status") == "ok")
        except Exception:
            return False

    # Async variants
    async def list_queues_async(self, vhost: str = "/") -> list[QueueInfo]:
        vhost_encoded = urllib.parse.quote(vhost, safe="")
        return cast("list[QueueInfo]", await self._request_async("GET", f"/queues/{vhost_encoded}"))

    async def get_queue_async(self, name: str, vhost: str = "/") -> QueueInfo:
        v = urllib.parse.quote(vhost, safe="")
        n = urllib.parse.quote(name, safe="")
        return cast("QueueInfo", await self._request_async("GET", f"/queues/{v}/{n}"))

    async def overview_async(self) -> OverviewInfo:
        return cast("OverviewInfo", await self._request_async("GET", "/overview"))

    async def health_check_async(self) -> bool:
        try:
            result = await self._request_async("GET", "/healthchecks/node")
            return bool(result.get("status") == "ok")
        except Exception:
            return False

    async def _read_capped_async(self, resp: Any) -> bytes:
        """Read the async response body in chunks, raising if it exceeds the cap."""
        total = 0
        chunks: list[bytes] = []
        while True:
            chunk = await resp.content.read(_READ_CHUNK)
            if not chunk:
                break
            total += len(chunk)
            if total > _MAX_RESPONSE_BYTES:
                raise ValueError(f"Management API response exceeded {_MAX_RESPONSE_BYTES} bytes")
            chunks.append(chunk)
        return b"".join(chunks)

    async def _get_session(self) -> Any:
        """Lazily create and reuse a single aiohttp.ClientSession (L-6)."""
        try:
            import aiohttp
        except ImportError:
            raise ImportError(
                "Async management API requires aiohttp. Install with: pip install rabbitkit[management]"
            ) from None
        if self._aiohttp_session is None:
            self._aiohttp_session = aiohttp.ClientSession()
        return self._aiohttp_session

    async def aclose(self) -> None:
        """Close the long-lived aiohttp session if one was created.

        ``*_async`` methods reuse a single ``aiohttp.ClientSession`` for
        connection pooling. Call ``aclose()`` at shutdown to release the
        underlying connector's sockets; otherwise the session is cleaned up
        when the event loop closes.
        """
        if self._aiohttp_session is not None:
            await self._aiohttp_session.close()
            self._aiohttp_session = None

    async def _request_async(self, method: str, path: str, body: bytes | None = None) -> Any:
        try:
            import aiohttp
        except ImportError:
            raise ImportError(
                "Async management API requires aiohttp. Install with: pip install rabbitkit[management]"
            ) from None
        session = await self._get_session()

        url = f"{self._config.url}/api{path}"
        headers = {"Authorization": self._auth_header, "Content-Type": "application/json"}
        async with session.request(
            method,
            url,
            headers=headers,
            data=body,
            timeout=aiohttp.ClientTimeout(total=self._config.timeout),
            allow_redirects=False,  # never follow — surface 3xx as an error (SSRF guard)
        ) as resp:
            if 300 <= resp.status < 400:
                raise ValueError(f"Unexpected redirect ({resp.status}) to {resp.headers.get('Location')}")
            if resp.status == 204:
                return None
            if not (200 <= resp.status < 300):
                resp.raise_for_status()  # match the sync path: raise on non-2xx
            return json.loads((await self._read_capped_async(resp)).decode())
