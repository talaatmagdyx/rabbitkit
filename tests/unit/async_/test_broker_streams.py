"""AsyncBroker stream routes: live consume arguments, offset tracking, stop()."""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, patch

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.streams import STREAM_OFFSET_ARG, StreamConsumerConfig
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import QueueType


class _Store:
    def __init__(self, stored: int | None = None) -> None:
        self.stored = stored

    def load(self, key: str) -> int | None:
        return self.stored

    def save(self, key: str, offset: int) -> None:
        self.stored = offset


def _transport() -> AsyncMock:
    transport = AsyncMock()
    transport.consume = AsyncMock(return_value="tag-1")
    return transport


def _delivery(offset: int, tag: int, acked: list[int]) -> RabbitMessage:
    message = RabbitMessage(body=b"{}", headers={"x-stream-offset": offset}, delivery_tag=tag)

    async def ack() -> None:
        acked.append(offset)

    message._ack_async_fn = ack
    message._nack_async_fn = AsyncMock()
    message._reject_async_fn = AsyncMock()
    return message


async def _started(broker: AsyncBroker, transport: AsyncMock) -> None:
    with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=transport):
        await broker.start(install_signal_handlers=False)


class TestStreamRouteStart:
    async def test_consumes_with_the_live_arguments_and_no_safety_dlq(self) -> None:
        broker = AsyncBroker()
        handled: list[bytes] = []

        @broker.subscriber(RabbitQueue("events", queue_type=QueueType.STREAM), stream_offset="first")
        async def handle(body: bytes) -> None:
            handled.append(body)

        transport = _transport()
        await _started(broker, transport)
        try:
            (tracker,) = broker._stream_trackers.values()
            kwargs = transport.consume.call_args.kwargs
            assert kwargs["arguments"] is tracker.consume_arguments
            assert kwargs["arguments"][STREAM_OFFSET_ARG] == "first"
            # Only the stream itself: a stream gets no x-dead-letter-exchange
            # and no auto-provisioned "<queue>.dlq" (RabbitMQ refuses both).
            assert transport.declare_queue.await_count == 1
            declared = transport.declare_queue.await_args.args[0]
            assert declared.dead_letter_exchange is None

            acked: list[int] = []
            await kwargs["callback"](_delivery(0, 1, acked))
            assert handled == [b"{}"]
            assert acked == [0]
            assert tracker.processed_offset == 0
            assert tracker.consume_arguments[STREAM_OFFSET_ARG] == 1
        finally:
            await broker.stop(timeout=2)
        assert tracker.committed_offset == 0, "stop() commits the processed offset"

    async def test_a_classic_route_consumes_without_arguments(self) -> None:
        broker = AsyncBroker()

        @broker.subscriber("orders")
        async def handle(body: bytes) -> None: ...

        transport = _transport()
        await _started(broker, transport)
        try:
            assert "arguments" not in transport.consume.call_args.kwargs
            assert broker._stream_trackers == {}
        finally:
            await broker.stop(timeout=2)

    async def test_a_configured_store_is_loaded_off_the_loop_and_resumed_from(self) -> None:
        broker = AsyncBroker()
        store = _Store(stored=99)

        @broker.subscriber(
            RabbitQueue("events", queue_type=QueueType.STREAM),
            stream=StreamConsumerConfig(offset="first", offset_store=store),
        )
        async def handle(body: bytes) -> None: ...

        transport = _transport()
        with patch("rabbitkit.async_.broker.asyncio.to_thread", wraps=asyncio.to_thread) as to_thread:
            await _started(broker, transport)
        try:
            assert to_thread.call_count >= 1
            assert transport.consume.call_args.kwargs["arguments"][STREAM_OFFSET_ARG] == 100
        finally:
            await broker.stop(timeout=2)

    async def test_the_tracker_survives_a_restart(self) -> None:
        broker = AsyncBroker()

        @broker.subscriber(RabbitQueue("events", queue_type=QueueType.STREAM), stream_offset="first")
        async def handle(body: bytes) -> None: ...

        transport = _transport()
        await _started(broker, transport)
        (tracker,) = broker._stream_trackers.values()
        await transport.consume.call_args.kwargs["callback"](_delivery(0, 1, []))
        await broker.stop(timeout=2)

        await _started(broker, _transport())
        try:
            assert list(broker._stream_trackers.values()) == [tracker]
            assert tracker.consume_arguments[STREAM_OFFSET_ARG] == 1
        finally:
            await broker.stop(timeout=2)


class TestStreamRouteStop:
    async def test_stop_hands_back_without_advancing_and_commits(self) -> None:
        broker = AsyncBroker()

        @broker.subscriber(RabbitQueue("events", queue_type=QueueType.STREAM))
        async def handle(body: bytes) -> None: ...

        transport = _transport()
        await _started(broker, transport)
        (tracker,) = broker._stream_trackers.values()
        message = _delivery(0, 1, [])
        tracker.track(message)
        await broker.stop(timeout=2)
        await message.nack_async(requeue=True)  # e.g. a pool hand-back during stop()
        assert tracker.processed_offset is None

    async def test_a_final_commit_that_overruns_the_deadline_is_abandoned(self) -> None:
        broker = AsyncBroker()

        @broker.subscriber(RabbitQueue("events", queue_type=QueueType.STREAM))
        async def handle(body: bytes) -> None: ...

        transport = _transport()
        await _started(broker, transport)
        (tracker,) = broker._stream_trackers.values()

        async def wedged() -> Any:
            await asyncio.sleep(60)

        with (
            patch.object(tracker, "flush_async", wedged),
            patch("rabbitkit.async_.broker.logger") as logger,
        ):
            await broker.stop(timeout=0.2)
        assert any("did not finish before the stop deadline" in call.args[0] for call in logger.warning.call_args_list)
