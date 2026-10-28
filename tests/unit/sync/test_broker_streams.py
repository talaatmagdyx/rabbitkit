"""SyncBroker stream routes: consume arguments per (re)subscription, tracking, stop()."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.streams import STREAM_OFFSET_ARG
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import QueueType
from rabbitkit.sync.broker import SyncBroker


@pytest.fixture(autouse=True)
def _check_pika() -> None:
    pytest.importorskip("pika")


class _Transport:
    """Records every consume() call with the arguments as they were then."""

    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []
        self.cancel_consumer = MagicMock()
        self.disconnect = MagicMock()
        # Broker-cancel hooks the SyncBroker wires on every start (G-26).
        self.on_consumer_cancelled = MagicMock()
        self.on_consumer_resumed = MagicMock()

    def consume(self, queue: str, callback: Any, prefetch: int = 10, **kw: Any) -> str:
        args = kw.get("arguments")
        self.calls.append({"queue": queue, "callback": callback, "arguments": None if args is None else dict(args)})
        return f"tag-{len(self.calls)}"


def _delivery(offset: int, tag: int, acked: list[int]) -> RabbitMessage:
    message = RabbitMessage(body=b"x", headers={"x-stream-offset": offset}, delivery_tag=tag)
    message._ack_fn = lambda: acked.append(offset)
    message._nack_fn = MagicMock()
    message._reject_fn = MagicMock()
    return message


def _broker(**subscriber_kw: Any) -> tuple[SyncBroker, list[bytes]]:
    broker = SyncBroker()
    handled: list[bytes] = []

    @broker.subscriber(RabbitQueue("events", queue_type=QueueType.STREAM), **subscriber_kw)
    def handle(body: bytes) -> None:
        handled.append(body)

    return broker, handled


class TestSyncStreamRoute:
    def test_resubscribes_from_the_processed_position(self) -> None:
        """_recover_consumers() calls _start_consumer() again: the second
        basic.consume carries the resume point, not the configured "first"."""
        broker, handled = _broker(stream_offset="first")
        transport = _Transport()
        broker._transport = transport  # type: ignore[assignment]
        route = broker.routes[0]

        broker._start_consumer(route)
        assert transport.calls[0]["arguments"] == {STREAM_OFFSET_ARG: "first"}

        acked: list[int] = []
        for offset in range(3):
            transport.calls[0]["callback"](_delivery(offset, offset + 1, acked))
        assert handled == [b"x"] * 3
        assert acked == [0, 1, 2]

        broker._start_consumer(route)  # what a reconnect does
        assert transport.calls[1]["arguments"] == {STREAM_OFFSET_ARG: 3}
        (tracker,) = broker._stream_trackers.values()
        assert tracker.processed_offset == 2

    def test_a_classic_route_consumes_without_arguments(self) -> None:
        broker = SyncBroker()

        @broker.subscriber("orders")
        def handle(body: bytes) -> None: ...

        transport = _Transport()
        broker._transport = transport  # type: ignore[assignment]
        broker._start_consumer(broker.routes[0])
        assert transport.calls[0]["arguments"] is None
        assert broker._stream_trackers == {}

    def test_stop_hands_back_without_advancing_and_commits(self) -> None:
        broker, _handled = _broker(stream_offset="first")
        transport = _Transport()
        broker._transport = transport  # type: ignore[assignment]
        broker._start_consumer(broker.routes[0])
        (tracker,) = broker._stream_trackers.values()

        acked: list[int] = []
        done, handed_back = _delivery(0, 1, acked), _delivery(1, 2, acked)
        tracker.track(done)
        tracker.track(handed_back)
        done.ack()
        broker._started = True
        broker.stop(timeout=1)
        handed_back.nack(requeue=True)

        assert tracker.processed_offset == 0
        assert tracker.committed_offset == 0, "stop() commits the processed offset"
        transport.disconnect.assert_called_once()
