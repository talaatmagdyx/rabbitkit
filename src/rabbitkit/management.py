"""RabbitMQ Management HTTP API client.

Provides a thin Python wrapper around the **RabbitMQ Management HTTP API**
(available on port 15672 by default).

* **Sync** operations use ``urllib.request`` — no extra dependencies.
* **Async** operations use ``aiohttp`` — requires ``pip install rabbitkit[management]``.

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

    client.list_queues(vhost="/")          -> list[dict]
    client.get_queue(name, vhost="/")      -> dict
    client.purge_queue(name, vhost="/")    -> None
    client.delete_queue(name, vhost="/")   -> None
    client.list_exchanges(vhost="/")       -> list[dict]
    client.list_connections()              -> list[dict]
    client.list_channels()                 -> list[dict]
    client.overview()                      -> dict
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
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ManagementConfig:
    """RabbitMQ Management API configuration."""

    url: str = "http://localhost:15672"
    username: str = "guest"
    password: str = "guest"
    timeout: float = 10.0


class RabbitManagementClient:
    """HTTP client for the RabbitMQ Management API."""

    def __init__(self, config: ManagementConfig | None = None) -> None:
        self._config = config or ManagementConfig()
        credentials = f"{self._config.username}:{self._config.password}"
        self._auth_header = "Basic " + base64.b64encode(credentials.encode()).decode()

    def _request(self, method: str, path: str, body: bytes | None = None) -> Any:
        url = f"{self._config.url}/api{path}"
        req = urllib.request.Request(url, method=method, data=body)  # noqa: S310
        req.add_header("Authorization", self._auth_header)
        req.add_header("Content-Type", "application/json")
        with urllib.request.urlopen(req, timeout=self._config.timeout) as resp:  # noqa: S310
            if resp.status == 204:
                return None
            return json.loads(resp.read().decode())

    # Queue operations
    def list_queues(self, vhost: str = "/") -> list[dict[str, Any]]:
        vhost_encoded = urllib.parse.quote(vhost, safe="")
        return self._request("GET", f"/queues/{vhost_encoded}")  # type: ignore[no-any-return]

    def get_queue(self, name: str, vhost: str = "/") -> dict[str, Any]:
        vhost_encoded = urllib.parse.quote(vhost, safe="")
        name_encoded = urllib.parse.quote(name, safe="")
        return self._request("GET", f"/queues/{vhost_encoded}/{name_encoded}")  # type: ignore[no-any-return]

    def purge_queue(self, name: str, vhost: str = "/") -> None:
        vhost_encoded = urllib.parse.quote(vhost, safe="")
        name_encoded = urllib.parse.quote(name, safe="")
        self._request("DELETE", f"/queues/{vhost_encoded}/{name_encoded}/contents")

    def delete_queue(self, name: str, vhost: str = "/") -> None:
        vhost_encoded = urllib.parse.quote(vhost, safe="")
        name_encoded = urllib.parse.quote(name, safe="")
        self._request("DELETE", f"/queues/{vhost_encoded}/{name_encoded}")

    # Exchange operations
    def list_exchanges(self, vhost: str = "/") -> list[dict[str, Any]]:
        vhost_encoded = urllib.parse.quote(vhost, safe="")
        return self._request("GET", f"/exchanges/{vhost_encoded}")  # type: ignore[no-any-return]

    # Connection/Channel
    def list_connections(self) -> list[dict[str, Any]]:
        return self._request("GET", "/connections")  # type: ignore[no-any-return]

    def list_channels(self) -> list[dict[str, Any]]:
        return self._request("GET", "/channels")  # type: ignore[no-any-return]

    # Overview
    def overview(self) -> dict[str, Any]:
        return self._request("GET", "/overview")  # type: ignore[no-any-return]

    def health_check(self) -> bool:
        try:
            result = self._request("GET", "/healthchecks/node")
            return bool(result.get("status") == "ok")
        except Exception:
            return False

    # Async variants
    async def list_queues_async(self, vhost: str = "/") -> list[dict[str, Any]]:
        return await self._request_async("GET", f"/queues/{urllib.parse.quote(vhost, safe='')}")  # type: ignore[no-any-return]

    async def get_queue_async(self, name: str, vhost: str = "/") -> dict[str, Any]:
        v = urllib.parse.quote(vhost, safe="")
        n = urllib.parse.quote(name, safe="")
        return await self._request_async("GET", f"/queues/{v}/{n}")  # type: ignore[no-any-return]

    async def overview_async(self) -> dict[str, Any]:
        return await self._request_async("GET", "/overview")  # type: ignore[no-any-return]

    async def health_check_async(self) -> bool:
        try:
            result = await self._request_async("GET", "/healthchecks/node")
            return bool(result.get("status") == "ok")
        except Exception:
            return False

    async def _request_async(self, method: str, path: str, body: bytes | None = None) -> Any:
        try:
            import aiohttp
        except ImportError:
            raise ImportError(
                "Async management API requires aiohttp. "
                "Install with: pip install rabbitkit[management]"
            ) from None

        url = f"{self._config.url}/api{path}"
        headers = {"Authorization": self._auth_header, "Content-Type": "application/json"}
        async with aiohttp.ClientSession() as session:
            async with session.request(
                method,
                url,
                headers=headers,
                data=body,
                timeout=aiohttp.ClientTimeout(total=self._config.timeout),
            ) as resp:
                if resp.status == 204:
                    return None
                return await resp.json()
