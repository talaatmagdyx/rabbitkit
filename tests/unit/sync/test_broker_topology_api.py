"""SyncBroker's public topology API (16.6) and verify_topology (R-D8)."""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest

from rabbitkit.core.config import RabbitConfig
from rabbitkit.core.errors import BrokerNotStartedError, ConfigurationError
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import TopologyMode
from rabbitkit.sync.broker import SyncBroker


def _broker(mode: TopologyMode = TopologyMode.AUTO_DECLARE) -> tuple[SyncBroker, MagicMock]:
    broker = SyncBroker(RabbitConfig(topology_mode=mode))
    transport = MagicMock()
    broker._transport = transport
    return broker, transport


def test_auto_declare_delegates_to_transport() -> None:
    broker, transport = _broker()
    broker.declare_exchange(RabbitExchange("ex"))
    broker.declare_queue(RabbitQueue("q"))
    broker.bind_queue("q", "ex", "k", {"x-match": "all"})
    broker.bind_exchange("dst", "src")
    transport.declare_exchange.assert_called_once()
    transport.declare_queue.assert_called_once()
    transport.bind_queue.assert_called_once_with(
        queue="q", exchange="ex", routing_key="k", arguments={"x-match": "all"}
    )
    transport.bind_exchange.assert_called_once_with(destination="dst", source="src", routing_key="", arguments=None)


@pytest.mark.parametrize("mode", [TopologyMode.PASSIVE_ONLY, TopologyMode.MANUAL])
def test_writes_refused_outside_auto_declare(mode: TopologyMode) -> None:
    broker, transport = _broker(mode)
    with pytest.raises(ConfigurationError):
        broker.declare_queue(RabbitQueue("q"))
    with pytest.raises(ConfigurationError):
        broker.bind_queue("q", "ex")
    assert transport.method_calls == []


def test_not_started_raises() -> None:
    with pytest.raises(BrokerNotStartedError):
        SyncBroker(RabbitConfig()).declare_exchange(RabbitExchange("ex"))


def test_verify_topology_without_client_is_unverified() -> None:
    broker = SyncBroker(RabbitConfig())
    broker.subscriber(RabbitQueue("q"))(lambda body: None)
    report = broker.verify_topology()
    assert report.ok and not report.fully_verified
