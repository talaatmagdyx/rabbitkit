"""AsyncBroker's public topology API (16.6) and verify_topology (R-D8)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import pytest

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.config import RabbitConfig
from rabbitkit.core.errors import BrokerNotStartedError, ConfigurationError
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import QueueType, TopologyMode


def _broker(mode: TopologyMode = TopologyMode.AUTO_DECLARE) -> tuple[AsyncBroker, AsyncMock]:
    broker = AsyncBroker(RabbitConfig(topology_mode=mode))
    transport = AsyncMock()
    broker._transport = transport
    return broker, transport


@pytest.mark.asyncio
async def test_auto_declare_delegates_to_transport() -> None:
    broker, transport = _broker()
    await broker.declare_exchange(RabbitExchange("ex"))
    await broker.declare_queue(RabbitQueue("q"))
    await broker.bind_queue("q", "ex", "k")
    await broker.bind_exchange("dst", "src", "k")
    transport.declare_exchange.assert_awaited_once()
    transport.declare_queue.assert_awaited_once_with(RabbitQueue("q"))
    transport.bind_queue.assert_awaited_once_with(queue="q", exchange="ex", routing_key="k", arguments=None)
    transport.bind_exchange.assert_awaited_once_with(destination="dst", source="src", routing_key="k", arguments=None)


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [TopologyMode.PASSIVE_ONLY, TopologyMode.MANUAL])
async def test_writes_refused_outside_auto_declare(mode: TopologyMode) -> None:
    broker, transport = _broker(mode)
    with pytest.raises(ConfigurationError):
        await broker.declare_queue(RabbitQueue("q"))
    with pytest.raises(ConfigurationError):
        await broker.declare_exchange(RabbitExchange("ex"))
    with pytest.raises(ConfigurationError):
        await broker.bind_queue("q", "ex")
    with pytest.raises(ConfigurationError):
        await broker.bind_exchange("a", "b")
    assert transport.method_calls == []


@pytest.mark.asyncio
async def test_passive_check_allowed_in_passive_only() -> None:
    broker, transport = _broker(TopologyMode.PASSIVE_ONLY)
    await broker.declare_queue(RabbitQueue("q", passive=True))
    transport.declare_queue.assert_awaited_once()


@pytest.mark.asyncio
async def test_not_started_raises() -> None:
    broker = AsyncBroker(RabbitConfig())
    with pytest.raises(BrokerNotStartedError):
        await broker.declare_queue(RabbitQueue("q"))


@pytest.mark.asyncio
async def test_verify_topology_uses_routes_and_vhost() -> None:
    broker = AsyncBroker(RabbitConfig(topology_mode=TopologyMode.PASSIVE_ONLY))

    @broker.subscriber(RabbitQueue("q", queue_type=QueueType.QUORUM))
    async def handle(body: bytes) -> None:  # pragma: no cover - not invoked
        pass

    seen: list[tuple[str, str]] = []

    class _Mgmt:
        def get_queue(self, name: str, vhost: str = "/") -> dict[str, Any]:
            seen.append((name, vhost))
            return {"type": "classic", "durable": True, "arguments": {}}

        def get_exchange(self, name: str, vhost: str = "/") -> dict[str, Any]:  # pragma: no cover
            return {}

        def get_queue_bindings(self, queue: str, vhost: str = "/") -> list[dict[str, Any]]:  # pragma: no cover
            return []

    report = await broker.verify_topology(_Mgmt(), vhost="/v")
    assert seen == [("q", "/v")]
    assert not report.ok and report.failed[0].check == "type"
