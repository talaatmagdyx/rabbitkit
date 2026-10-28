"""AsyncBroker.publish()/stop() publisher contract: admission closed by
stop(), per-call timeout, kwargs-form message_id, and the
byte budget / batch wiring."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.config import BatchPublishConfig, PoolConfig, PublisherConfig, RabbitConfig
from rabbitkit.core.errors import BrokerNotStartedError
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus


def _mock_transport() -> AsyncMock:
    transport = AsyncMock()
    transport.publish = AsyncMock(return_value=PublishOutcome(status=PublishStatus.CONFIRMED))
    transport.byte_limiter = None
    return transport


async def _started(config: RabbitConfig | None = None, **kwargs: Any) -> tuple[AsyncBroker, AsyncMock]:
    broker = AsyncBroker(config, **kwargs)
    transport = _mock_transport()
    with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=transport) as cls:
        await broker.start(install_signal_handlers=False)
    broker._transport_cls_kwargs = cls.call_args.kwargs  # type: ignore[attr-defined]
    return broker, transport


class TestPublishAfterStop:
    @pytest.mark.asyncio
    async def test_publish_after_stop_raises_and_never_reconnects(self) -> None:
        """A publish after stop() used to reconnect silently and
        report CONFIRMED."""
        broker, transport = await _started()
        await broker.stop()
        with pytest.raises(BrokerNotStartedError):
            await broker.publish(MessageEnvelope(routing_key="q", body=b"x"))
        transport.publish.assert_not_awaited()
        transport.connect.assert_awaited_once()  # only the start() connect

    @pytest.mark.asyncio
    async def test_iter_publish_after_stop_raises(self) -> None:
        broker, _ = await _started()
        await broker.stop()
        with pytest.raises(BrokerNotStartedError):
            broker.iter_publish([MessageEnvelope(routing_key="q", body=b"x")])

    @pytest.mark.asyncio
    async def test_restart_reopens_admission(self) -> None:
        broker, _ = await _started()
        await broker.stop()
        transport = _mock_transport()
        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=transport):
            await broker.start(install_signal_handlers=False)
        assert (await broker.publish(routing_key="q", body=b"x")).ok
        await broker.stop()

    @pytest.mark.asyncio
    async def test_stop_closes_the_publisher_within_the_deadline(self) -> None:
        broker, transport = await _started()
        await broker.stop(timeout=3.0)
        transport.close_publishing.assert_awaited_once()
        remaining = transport.close_publishing.call_args.kwargs["timeout"]
        assert 0 < remaining <= 3.0
        assert transport.close_publishing.await_args_list  # before disconnect
        transport.disconnect.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_stop_hands_the_remaining_deadline_to_the_batch_publisher(self) -> None:
        broker, _ = await _started(batch_config=BatchPublishConfig(flush_workers=1))
        batch = broker._batch_publisher
        assert batch is not None
        with patch.object(batch, "stop", new=AsyncMock()) as stop:
            await broker.stop(timeout=2.0)
        assert 0 < stop.call_args.kwargs["timeout"] <= 2.0


class TestPerCallTimeout:
    @pytest.mark.asyncio
    async def test_rejects_a_non_positive_timeout(self) -> None:
        broker, _ = await _started()
        with pytest.raises(ValueError, match="timeout"):
            await broker.publish(routing_key="q", body=b"x", timeout=0)

    @pytest.mark.asyncio
    async def test_remaining_time_reaches_the_transport(self) -> None:
        broker, transport = await _started()
        await broker.publish(routing_key="q", body=b"x", timeout=2.0)
        assert 0 < transport.publish.call_args.kwargs["timeout"] <= 2.0

    @pytest.mark.asyncio
    async def test_without_a_timeout_the_transport_signature_is_unchanged(self) -> None:
        broker, transport = await _started()
        await broker.publish(routing_key="q", body=b"x")
        assert transport.publish.call_args.kwargs == {}

    @pytest.mark.asyncio
    async def test_deadline_spent_in_middleware_is_not_sent(self) -> None:
        from rabbitkit.middleware.base import BaseMiddleware

        class Slow(BaseMiddleware):
            async def publish_scope_async(self, call_next: Any, envelope: MessageEnvelope) -> Any:
                await asyncio.sleep(0.05)
                return await call_next(envelope)

        broker, transport = await _started(middlewares=[Slow()])
        outcome = await broker.publish(routing_key="q", body=b"x", timeout=0.01)
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "admission_timeout")
        transport.publish.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_flow_controller_wait_is_bounded_by_the_deadline(self) -> None:
        broker, transport = await _started()
        fc = AsyncMock()
        fc.acquire_async = AsyncMock(return_value=False)
        broker._flow_controller = fc
        outcome = await broker.publish(routing_key="q", body=b"x", timeout=1.0)
        assert 0 <= fc.acquire_async.call_args.kwargs["timeout"] <= 1.0
        assert (outcome.status, outcome.reason) == (PublishStatus.NOT_SENT, "backpressure_dropped")
        transport.publish.assert_not_awaited()


class TestMessageId:
    @pytest.mark.asyncio
    async def test_kwargs_form_uses_the_given_message_id(self) -> None:
        broker, transport = await _started()
        await broker.publish(routing_key="q", body=b"x", message_id="order-42")
        await broker.publish(routing_key="q", body=b"x", message_id="order-42")
        sent = [c.args[0] for c in transport.publish.call_args_list]
        assert [e.message_id for e in sent] == ["order-42", "order-42"]

    @pytest.mark.asyncio
    async def test_kwargs_form_without_message_id_gets_a_fresh_one(self) -> None:
        broker, transport = await _started()
        await broker.publish(routing_key="q", body=b"x")
        await broker.publish(routing_key="q", body=b"x")
        ids = {c.args[0].message_id for c in transport.publish.call_args_list}
        assert len(ids) == 2

    @pytest.mark.asyncio
    async def test_message_id_with_an_envelope_is_rejected(self) -> None:
        broker, _ = await _started()
        with pytest.raises(ValueError, match="message_id"):
            await broker.publish(MessageEnvelope(routing_key="q", body=b"x"), message_id="m")


class TestPublisherWiring:
    @pytest.mark.asyncio
    async def test_byte_budget_and_admission_reach_the_transport_and_batch(self) -> None:
        config = RabbitConfig(
            publisher=PublisherConfig(max_message_bytes=1000, max_in_flight_bytes=4096, confirm_timeout=2.5),
            pool=PoolConfig(channel_acquire_timeout=3.5),
        )
        broker, transport = await _started(config, batch_config=BatchPublishConfig(flush_workers=1))
        assert broker._transport_cls_kwargs["max_in_flight_bytes"] == 4096  # type: ignore[attr-defined]
        batch = broker._batch_publisher
        assert batch is not None
        assert batch._admission_timeout == 3.5
        assert batch._confirm_timeout == 2.5
        assert batch._byte_limiter is transport.byte_limiter
        await broker.stop()
