"""Tests for core/protocols.py — Transport ABCs and capability sub-protocols."""

from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.protocols import (
    AsyncCircuitBreakerProtocol,
    AsyncTransport,
    CircuitBreakerProtocol,
    MetricsCollector,
    SupportsBackpressure,
    SupportsPublisherConfirms,
    SupportsRPC,
    Transport,
)
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus

# ── helpers ───────────────────────────────────────────────────────────────


class ConcreteTransport:
    """Minimal Transport implementation for protocol checking."""

    def connect(self) -> None:
        pass

    def disconnect(self) -> None:
        pass

    def is_connected(self) -> bool:
        return True

    def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        return PublishOutcome(status=PublishStatus.CONFIRMED)

    def consume(
        self,
        queue: str,
        callback: Callable[[RabbitMessage], None],
        prefetch: int = 10,
        *,
        no_ack: bool = False,
        declare: bool = True,
    ) -> str:
        return "ctag.1"

    def declare_exchange(self, exchange: RabbitExchange) -> None:
        pass

    def declare_queue(self, queue: RabbitQueue) -> None:
        pass

    def bind_queue(self, queue: str, exchange: str, routing_key: str) -> None:
        pass

    def bind_exchange(
        self,
        destination: str,
        source: str,
        routing_key: str = "",
        arguments: dict[str, Any] | None = None,
    ) -> None:
        pass

    def cancel_consumer(self, consumer_tag: str) -> None:
        pass


class ConcreteAsyncTransport:
    """Minimal AsyncTransport implementation for protocol checking."""

    async def connect(self) -> None:
        pass

    async def disconnect(self) -> None:
        pass

    def is_connected(self) -> bool:
        return True

    async def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        return PublishOutcome(status=PublishStatus.CONFIRMED)

    async def consume(
        self,
        queue: str,
        callback: Callable[[RabbitMessage], Awaitable[None]],
        prefetch: int = 10,
        *,
        no_ack: bool = False,
        declare: bool = True,
    ) -> str:
        return "ctag.1"

    async def declare_exchange(self, exchange: RabbitExchange) -> None:
        pass

    async def declare_queue(self, queue: RabbitQueue) -> None:
        pass

    async def bind_queue(self, queue: str, exchange: str, routing_key: str) -> None:
        pass

    async def bind_exchange(
        self,
        destination: str,
        source: str,
        routing_key: str = "",
        arguments: dict[str, Any] | None = None,
    ) -> None:
        pass

    async def cancel_consumer(self, consumer_tag: str) -> None:
        pass


class ConcreteConfirms:
    """Satisfies SupportsPublisherConfirms."""

    def enable_confirms(self) -> None:
        pass


class ConcreteBackpressure:
    """Satisfies SupportsBackpressure."""

    def on_blocked(self, callback: Callable[[], None]) -> None:
        pass

    def on_unblocked(self, callback: Callable[[], None]) -> None:
        pass


class ConcreteRPC:
    """Satisfies SupportsRPC."""

    def create_reply_queue(self) -> str:
        return "amq.gen-xxx"


class ConcreteCB:
    """Satisfies CircuitBreakerProtocol."""

    def call(self, func: Callable[..., Any], *args: Any, **kwargs: Any) -> Any:
        return func(*args, **kwargs)


class ConcreteAsyncCB:
    """Satisfies AsyncCircuitBreakerProtocol."""

    async def call_async(self, func: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
        return await func(*args, **kwargs)


class ConcreteMetrics:
    """Satisfies MetricsCollector."""

    def increment(self, name: str, tags: dict[str, str] | None = None) -> None:
        pass

    def histogram(self, name: str, value: float, tags: dict[str, str] | None = None) -> None:
        pass


# ── Transport protocol ───────────────────────────────────────────────────


class TestTransportProtocol:
    def test_isinstance_check(self) -> None:
        t = ConcreteTransport()
        assert isinstance(t, Transport)

    def test_non_conforming_fails(self) -> None:
        class Incomplete:
            def connect(self) -> None:
                pass

        assert not isinstance(Incomplete(), Transport)

    def test_connect_disconnect(self) -> None:
        t = ConcreteTransport()
        t.connect()
        assert t.is_connected()
        t.disconnect()

    def test_publish(self) -> None:
        t = ConcreteTransport()
        env = MessageEnvelope(routing_key="rk", body=b"hello")
        outcome = t.publish(env)
        assert outcome.ok

    def test_consume(self) -> None:
        t = ConcreteTransport()
        tag = t.consume("q", lambda msg: None)
        assert tag == "ctag.1"

    def test_declare_exchange(self) -> None:
        t = ConcreteTransport()
        ex = RabbitExchange(name="test")
        t.declare_exchange(ex)  # no exception

    def test_declare_queue(self) -> None:
        t = ConcreteTransport()
        q = RabbitQueue(name="test")
        t.declare_queue(q)  # no exception

    def test_bind_queue(self) -> None:
        t = ConcreteTransport()
        t.bind_queue("q", "ex", "rk")  # no exception

    def test_cancel_consumer(self) -> None:
        t = ConcreteTransport()
        t.cancel_consumer("ctag.1")  # no exception


# ── AsyncTransport protocol ──────────────────────────────────────────────


class TestAsyncTransportProtocol:
    def test_isinstance_check(self) -> None:
        t = ConcreteAsyncTransport()
        assert isinstance(t, AsyncTransport)

    def test_non_conforming_fails(self) -> None:
        class Incomplete:
            async def connect(self) -> None:
                pass

        assert not isinstance(Incomplete(), AsyncTransport)

    @pytest.mark.asyncio
    async def test_connect_disconnect(self) -> None:
        t = ConcreteAsyncTransport()
        await t.connect()
        assert t.is_connected()
        await t.disconnect()

    @pytest.mark.asyncio
    async def test_publish(self) -> None:
        t = ConcreteAsyncTransport()
        env = MessageEnvelope(routing_key="rk", body=b"hello")
        outcome = await t.publish(env)
        assert outcome.ok

    @pytest.mark.asyncio
    async def test_consume(self) -> None:
        t = ConcreteAsyncTransport()

        async def callback(msg: RabbitMessage) -> None:
            pass

        tag = await t.consume("q", callback)
        assert tag == "ctag.1"

    @pytest.mark.asyncio
    async def test_declare_exchange(self) -> None:
        t = ConcreteAsyncTransport()
        ex = RabbitExchange(name="test")
        await t.declare_exchange(ex)

    @pytest.mark.asyncio
    async def test_declare_queue(self) -> None:
        t = ConcreteAsyncTransport()
        q = RabbitQueue(name="test")
        await t.declare_queue(q)

    @pytest.mark.asyncio
    async def test_bind_queue(self) -> None:
        t = ConcreteAsyncTransport()
        await t.bind_queue("q", "ex", "rk")

    @pytest.mark.asyncio
    async def test_cancel_consumer(self) -> None:
        t = ConcreteAsyncTransport()
        await t.cancel_consumer("ctag.1")


# ── Capability sub-protocols ─────────────────────────────────────────────


class TestSupportsPublisherConfirms:
    def test_isinstance_check(self) -> None:
        assert isinstance(ConcreteConfirms(), SupportsPublisherConfirms)

    def test_non_conforming(self) -> None:
        assert not isinstance(object(), SupportsPublisherConfirms)


class TestSupportsBackpressure:
    def test_isinstance_check(self) -> None:
        assert isinstance(ConcreteBackpressure(), SupportsBackpressure)

    def test_non_conforming(self) -> None:
        assert not isinstance(object(), SupportsBackpressure)


class TestSupportsRPC:
    def test_isinstance_check(self) -> None:
        assert isinstance(ConcreteRPC(), SupportsRPC)

    def test_create_reply_queue(self) -> None:
        rpc = ConcreteRPC()
        name = rpc.create_reply_queue()
        assert isinstance(name, str)
        assert len(name) > 0

    def test_non_conforming(self) -> None:
        assert not isinstance(object(), SupportsRPC)


# ── Circuit breaker protocols ────────────────────────────────────────────


class TestCircuitBreakerProtocol:
    def test_isinstance_check(self) -> None:
        assert isinstance(ConcreteCB(), CircuitBreakerProtocol)

    def test_call_executes_function(self) -> None:
        cb = ConcreteCB()
        result = cb.call(lambda x: x * 2, 5)
        assert result == 10

    def test_non_conforming(self) -> None:
        assert not isinstance(object(), CircuitBreakerProtocol)


class TestAsyncCircuitBreakerProtocol:
    def test_isinstance_check(self) -> None:
        assert isinstance(ConcreteAsyncCB(), AsyncCircuitBreakerProtocol)

    @pytest.mark.asyncio
    async def test_call_async_executes(self) -> None:
        cb = ConcreteAsyncCB()

        async def double(x: int) -> int:
            return x * 2

        result = await cb.call_async(double, 5)
        assert result == 10

    def test_non_conforming(self) -> None:
        assert not isinstance(object(), AsyncCircuitBreakerProtocol)


# ── Metrics protocols ────────────────────────────────────────────────────


class TestMetricsCollector:
    def test_isinstance_check(self) -> None:
        assert isinstance(ConcreteMetrics(), MetricsCollector)

    def test_increment(self) -> None:
        m = ConcreteMetrics()
        m.increment("counter", tags={"env": "test"})  # no exception

    def test_histogram(self) -> None:
        m = ConcreteMetrics()
        m.histogram("latency", 0.5, tags={"env": "test"})  # no exception

    def test_non_conforming(self) -> None:
        assert not isinstance(object(), MetricsCollector)


# ── Composed protocol checks ────────────────────────────────────────────


class TestComposedProtocol:
    def test_transport_with_confirms(self) -> None:
        """A transport can also satisfy SupportsPublisherConfirms."""

        class FullTransport(ConcreteTransport, ConcreteConfirms):
            pass

        t = FullTransport()
        assert isinstance(t, Transport)
        assert isinstance(t, SupportsPublisherConfirms)

    def test_transport_with_rpc(self) -> None:
        class RPCTransport(ConcreteTransport, ConcreteRPC):
            pass

        t = RPCTransport()
        assert isinstance(t, Transport)
        assert isinstance(t, SupportsRPC)

    def test_transport_with_all_capabilities(self) -> None:
        class SuperTransport(ConcreteTransport, ConcreteConfirms, ConcreteBackpressure, ConcreteRPC):
            pass

        t = SuperTransport()
        assert isinstance(t, Transport)
        assert isinstance(t, SupportsPublisherConfirms)
        assert isinstance(t, SupportsBackpressure)
        assert isinstance(t, SupportsRPC)
