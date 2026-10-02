"""Quorum DLQs and RabbitMQ 4.x's default delivery limit (issue #32).

rabbitkit's DLQs have no dead-letter exchange. On 4.x a quorum queue
defaults to a delivery limit of 20, so the 21st requeue of a DLQ message
(20 peeks) deleted it. New quorum DLQs on 4.x are declared with
``x-delivery-limit: -1``; existing ones are left alone, because adding the
argument to an existing queue is a 406.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.config import RabbitConfig, RetryConfig
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import QueueType, TopologyMode
from rabbitkit.middleware.retry import RetryRouter, dlq_queue_definition
from rabbitkit.sync.broker import SyncBroker


class TestDlqQueueDefinition:
    def test_quorum_on_4x_is_unlimited(self) -> None:
        q = dlq_queue_definition("orders.dlq", quorum=True, broker_version="4.1.8")
        assert q.queue_type is QueueType.QUORUM
        assert q.delivery_limit == -1
        assert q.to_declare_kwargs()["arguments"]["x-delivery-limit"] == -1

    def test_quorum_on_3x_has_no_limit_argument(self) -> None:
        """-1 drops a message on its first return on 3.x."""
        q = dlq_queue_definition("orders.dlq", quorum=True, broker_version="3.13.7")
        assert q.delivery_limit is None
        assert "x-delivery-limit" not in q.to_declare_kwargs().get("arguments", {})

    def test_unknown_version_has_no_limit_argument(self) -> None:
        assert dlq_queue_definition("d", quorum=True, broker_version=None).delivery_limit is None

    def test_classic(self) -> None:
        q = dlq_queue_definition("d", quorum=False, broker_version="4.1.8")
        assert q.queue_type is QueueType.CLASSIC and q.delivery_limit is None


class TestRetryRouterDlq:
    def test_inherited_quorum_dlq_on_4x(self) -> None:
        router = RetryRouter(RetryConfig(max_retries=1, delays=(1,)))
        dlq = router.get_delay_queue_definitions(
            "orders", "", source_queue_type=QueueType.QUORUM, broker_version="4.0.2"
        )[-1]
        assert dlq.name == "orders.dlq" and dlq.delivery_limit == -1

    def test_default_version_is_backward_compatible(self) -> None:
        router = RetryRouter(RetryConfig(max_retries=1, delays=(1,)))
        dlq = router.get_delay_queue_definitions("orders", "", source_queue_type=QueueType.QUORUM)[-1]
        assert dlq.delivery_limit is None


def _quorum_route(broker: SyncBroker | AsyncBroker, *, retry: bool) -> None:
    kwargs = {"retry": RetryConfig(max_retries=1, delays=(1,))} if retry else {}

    @broker.subscriber(queue=RabbitQueue(name="orders", queue_type=QueueType.QUORUM), **kwargs)
    def handle(body: bytes) -> None:
        pass


def _sync_transport(*, version: str | None, exists: bool) -> MagicMock:
    transport = MagicMock()
    transport.server_version = version
    transport.queue_exists.return_value = exists
    return transport


def _declared(transport: MagicMock) -> dict[str, RabbitQueue]:
    return {c.args[0].name: c.args[0] for c in transport.declare_queue.call_args_list}


class TestSyncBrokerDeclaresDlq:
    @pytest.mark.parametrize("retry", [True, False], ids=["retry-dlq", "safety-dlq"])
    def test_new_quorum_dlq_on_4x_is_declared_unlimited(self, retry: bool) -> None:
        broker = SyncBroker()
        _quorum_route(broker, retry=retry)
        broker._transport = _sync_transport(version="4.1.8", exists=False)
        broker._declare_topology()
        broker._transport.queue_exists.assert_called_once_with("orders.dlq")
        assert _declared(broker._transport)["orders.dlq"].delivery_limit == -1

    @pytest.mark.parametrize("retry", [True, False], ids=["retry-dlq", "safety-dlq"])
    def test_existing_quorum_dlq_is_not_redeclared(self, retry: bool) -> None:
        broker = SyncBroker()
        _quorum_route(broker, retry=retry)
        broker._transport = _sync_transport(version="4.1.8", exists=True)
        broker._declare_topology()
        assert "orders.dlq" not in _declared(broker._transport)
        assert "orders" in _declared(broker._transport)  # the source still is

    def test_3x_new_dlq_has_no_limit_argument(self) -> None:
        broker = SyncBroker()
        _quorum_route(broker, retry=True)
        broker._transport = _sync_transport(version="3.13.7", exists=False)
        broker._declare_topology()
        assert _declared(broker._transport)["orders.dlq"].delivery_limit is None

    def test_existing_dlq_left_alone_even_when_the_version_is_unknown(self) -> None:
        """It may carry -1 from an earlier run on 4.x; a plain redeclare is a 406."""
        broker = SyncBroker()
        _quorum_route(broker, retry=True)
        broker._transport = _sync_transport(version=None, exists=True)
        broker._declare_topology()
        assert "orders.dlq" not in _declared(broker._transport)

    def test_mock_transport_without_a_version_string(self) -> None:
        broker = SyncBroker()
        _quorum_route(broker, retry=True)
        broker._transport = MagicMock()  # queue_exists returns a MagicMock, not True
        broker._declare_topology()
        assert _declared(broker._transport)["orders.dlq"].delivery_limit is None

    def test_classic_dlq_is_never_probed(self) -> None:
        broker = SyncBroker()

        @broker.subscriber(queue="orders", retry=RetryConfig(max_retries=1, delays=(1,)))
        def handle(body: bytes) -> None:
            pass

        broker._transport = _sync_transport(version="4.1.8", exists=True)
        broker._declare_topology()
        broker._transport.queue_exists.assert_not_called()
        assert "orders.dlq" in _declared(broker._transport)

    @pytest.mark.parametrize("consumer_first", [False, True])
    def test_a_route_consuming_another_routes_quorum_dlq_declares_it_the_same_way(
        self, consumer_first: bool
    ) -> None:
        """Review finding: the DLQ-consumer route redeclared orders.dlq without
        x-delivery-limit, a 406 against the -1 the retry topology created (or,
        consumer first, a DLQ stuck at 4.x's default limit of 20)."""
        broker = SyncBroker()

        def register_consumer() -> None:
            @broker.subscriber(queue=RabbitQueue(name="orders.dlq", queue_type=QueueType.QUORUM))
            def drain(body: bytes) -> None:
                pass

        if consumer_first:
            register_consumer()
        _quorum_route(broker, retry=True)
        if not consumer_first:
            register_consumer()
        transport = _sync_transport(version="4.1.8", exists=False)
        created: set[str] = set()
        transport.queue_exists.side_effect = lambda name: name in created
        transport.declare_queue.side_effect = lambda q: created.add(q.name)
        broker._transport = transport
        broker._declare_topology()
        dlq_declares = [c.args[0] for c in transport.declare_queue.call_args_list if c.args[0].name == "orders.dlq"]
        assert len(dlq_declares) == 1  # the second declaration saw it exists
        assert dlq_declares[0].delivery_limit == -1

    def test_passive_mode_never_probes(self) -> None:
        broker = SyncBroker(RabbitConfig(topology_mode=TopologyMode.PASSIVE_ONLY))
        _quorum_route(broker, retry=True)
        broker._transport = _sync_transport(version="4.1.8", exists=True)
        broker._declare_topology()
        broker._transport.queue_exists.assert_not_called()


class TestAsyncBrokerDeclaresDlq:
    def _transport(self, *, version: str | None, exists: bool) -> AsyncMock:
        transport = AsyncMock()
        transport.server_version = version
        transport.queue_exists.return_value = exists
        return transport

    @pytest.mark.parametrize("retry", [True, False], ids=["retry-dlq", "safety-dlq"])
    async def test_new_quorum_dlq_on_4x_is_declared_unlimited(self, retry: bool) -> None:
        broker = AsyncBroker()
        _quorum_route(broker, retry=retry)
        broker._transport = self._transport(version="4.1.8", exists=False)
        await broker._declare_topology()
        broker._transport.queue_exists.assert_awaited_once_with("orders.dlq")
        assert _declared(broker._transport)["orders.dlq"].delivery_limit == -1

    async def test_existing_quorum_dlq_is_not_redeclared(self) -> None:
        broker = AsyncBroker()
        _quorum_route(broker, retry=True)
        broker._transport = self._transport(version="4.1.8", exists=True)
        await broker._declare_topology()
        assert "orders.dlq" not in _declared(broker._transport)

    async def test_3x_new_dlq_has_no_limit_argument(self) -> None:
        broker = AsyncBroker()
        _quorum_route(broker, retry=True)
        broker._transport = self._transport(version="3.13.7", exists=False)
        await broker._declare_topology()
        assert _declared(broker._transport)["orders.dlq"].delivery_limit is None

    async def test_a_route_consuming_another_routes_quorum_dlq(self) -> None:
        broker = AsyncBroker()
        _quorum_route(broker, retry=True)

        @broker.subscriber(queue=RabbitQueue(name="orders.dlq", queue_type=QueueType.QUORUM))
        async def drain(body: bytes) -> None:
            pass

        transport = self._transport(version="4.1.8", exists=False)
        created: set[str] = set()

        async def exists(name: str) -> bool:
            return name in created

        async def declare(q: RabbitQueue) -> None:
            created.add(q.name)

        transport.queue_exists.side_effect = exists
        transport.declare_queue.side_effect = declare
        broker._transport = transport
        await broker._declare_topology()
        dlq_declares = [c.args[0] for c in transport.declare_queue.call_args_list if c.args[0].name == "orders.dlq"]
        assert [q.delivery_limit for q in dlq_declares] == [-1]
