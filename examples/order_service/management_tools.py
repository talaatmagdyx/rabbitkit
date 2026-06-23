"""Management-API helpers (docs §30) — queue stats and DLQ-depth checks that come
from the broker (HTTP API), so they keep working even when the app is down."""

from __future__ import annotations

from rabbitkit import ManagementConfig, RabbitManagementClient


def build_client(url: str, username: str, password: str) -> RabbitManagementClient:
    return RabbitManagementClient(
        ManagementConfig(url=url, username=username, password=password, timeout=10.0)
    )


def dlq_depth(client: RabbitManagementClient, queue: str, vhost: str = "/orders") -> int:
    """Current message count in a queue's DLQ (drives the keystone alert, docs §22)."""
    info = client.get_queue(f"{queue}.dlq", vhost=vhost)
    return int(info.get("messages", 0))


def assert_healthy(client: RabbitManagementClient) -> None:
    if not client.health_check():
        raise RuntimeError("RabbitMQ management health check failed")
