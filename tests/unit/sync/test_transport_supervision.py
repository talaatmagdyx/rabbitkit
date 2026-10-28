"""SyncTransport: broker-initiated consumer cancel (sync), the consume
loop's poll interval, and the publish in-flight counter (sync)."""

from __future__ import annotations

import threading
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

from rabbitkit.core.config import ConnectionConfig
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.sync.transport import SyncTransport

pika = pytest.importorskip("pika")


def _connected() -> tuple[SyncTransport, MagicMock, MagicMock]:
    """A transport on a mocked connection; returns (transport, connection, channel)."""
    transport = SyncTransport(connection_config=ConnectionConfig())
    channel = MagicMock()
    channel.is_open = True
    with patch("rabbitkit.sync.transport.make_pika_connection_params"), patch("pika.BlockingConnection") as conn:
        conn.return_value.channel.return_value = channel
        conn.return_value.is_open = True
        transport.connect()
    return transport, transport._connection, channel


def _cancel_frame(tag: str) -> Any:
    frame = MagicMock()
    frame.method.consumer_tag = tag
    return frame


def _cancel(transport: SyncTransport, channel: MagicMock, tag: str) -> None:
    """Deliver a server basic.cancel through the callback consume() registered."""
    callback = channel.add_on_cancel_callback.call_args.args[0]
    callback(_cancel_frame(tag))


class TestBrokerCancelDetection:
    def test_consume_registers_the_cancel_callback_and_a_record(self) -> None:
        transport, _conn, channel = _connected()
        tag = transport.consume("orders", lambda m: None, prefetch=7)
        channel.add_on_cancel_callback.assert_called_once_with(transport._on_broker_cancel)
        record = transport._consumer_supervisor.record("orders")
        assert record is not None
        assert (record.tag, record.prefetch, record.supervised) == (tag, 7, True)

    def test_a_server_cancel_is_reported_through_the_hooks(self) -> None:
        transport, _conn, channel = _connected()
        events: list[str] = []
        transport.on_consumer_cancelled(lambda q: events.append(f"cancelled:{q}"))
        transport.on_consumer_resumed(lambda q: events.append(f"resumed:{q}"))
        tag = transport.consume("orders", lambda m: None)
        _cancel(transport, channel, tag)
        assert events == ["cancelled:orders"]
        assert transport.cancelled_consumers == frozenset({"orders"})

    def test_a_frame_without_a_tag_is_ignored(self) -> None:
        transport, _conn, _channel = _connected()
        transport.consume("orders", lambda m: None)
        transport._on_broker_cancel(object())
        assert transport.cancelled_consumers == frozenset()

    def test_reply_to_consumers_are_not_supervised(self) -> None:
        transport, _conn, channel = _connected()
        tag = transport.consume("amq.rabbitmq.reply-to", lambda m: None, no_ack=True, declare=False)
        _cancel(transport, channel, tag)
        assert transport.cancelled_consumers == frozenset()


class TestResubscribe:
    def _cancelled(self) -> tuple[SyncTransport, MagicMock, MagicMock, str]:
        transport, conn, channel = _connected()
        tag = transport.consume("orders", lambda m: None, prefetch=5)
        _cancel(transport, channel, tag)
        record = transport._consumer_supervisor.record("orders")
        assert record is not None
        record.next_attempt = 0.0  # due now
        return transport, conn, channel, tag

    def test_waits_while_the_queue_is_missing(self) -> None:
        transport, conn, channel, _tag = self._cancelled()
        probe = MagicMock()
        probe.is_open = False
        probe.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(404, "NOT_FOUND")
        conn.channel.return_value = probe
        transport._consumer_supervisor.check_once()
        probe.queue_declare.assert_called_once_with(queue="orders", passive=True)
        assert channel.basic_consume.call_count == 1  # only the original subscribe
        assert transport.cancelled_consumers == frozenset({"orders"})

    def test_resubscribes_on_the_same_open_channel_with_the_same_tag(self) -> None:
        transport, conn, channel, tag = self._cancelled()
        probe = MagicMock()
        probe.is_open = True
        conn.channel.return_value = probe
        resumed: list[str] = []
        transport.on_consumer_resumed(resumed.append)
        transport._consumer_supervisor.check_once()
        probe.close.assert_called_once()
        probe.queue_declare.assert_called_once_with(queue="orders", passive=True)  # passive: never re-created
        assert channel.basic_consume.call_count == 2
        kwargs = channel.basic_consume.call_args.kwargs
        assert kwargs["queue"] == "orders" and kwargs["consumer_tag"] == tag and kwargs["auto_ack"] is False
        assert resumed == ["orders"]
        assert transport.cancelled_consumers == frozenset()

    def test_a_stream_consumer_resubscribes_at_its_current_offset(self) -> None:
        transport, conn, channel = _connected()
        live: dict[str, object] = {"x-stream-offset": "first"}
        tag = transport.consume("events", lambda m: None, arguments=live)
        assert channel.basic_consume.call_args.kwargs["arguments"] == {"x-stream-offset": "first"}
        _cancel(transport, channel, tag)
        record = transport._consumer_supervisor.record("events")
        assert record is not None
        record.next_attempt = 0.0
        live["x-stream-offset"] = 42  # the offset tracker advanced while cancelled
        probe = MagicMock()
        probe.is_open = True
        conn.channel.return_value = probe
        transport._consumer_supervisor.check_once()
        assert channel.basic_consume.call_args.kwargs["arguments"] == {"x-stream-offset": 42}

    def test_opens_a_fresh_channel_when_the_broker_closed_it(self) -> None:
        transport, conn, _channel, tag = self._cancelled()
        transport._consumer_channels["orders"] = MagicMock(is_open=False)  # closed by the broker
        fresh = MagicMock()
        fresh.is_open = True
        conn.channel.return_value = fresh
        opened: list[int] = []
        rebuilt: list[int] = []
        transport.on_channel_opened(lambda: opened.append(1))
        transport.on_channel_rebuilt(lambda: rebuilt.append(1))
        transport._consumer_supervisor.check_once()
        fresh.basic_qos.assert_called_once_with(prefetch_count=5)
        fresh.add_on_cancel_callback.assert_called_once_with(transport._on_broker_cancel)
        assert fresh.basic_consume.call_args.kwargs["consumer_tag"] == tag
        assert transport._consumer_channels["orders"] is fresh
        assert (opened, rebuilt) == ([1], [1])

    def test_a_duplicate_tag_counts_as_consuming(self) -> None:
        transport, conn, channel, _tag = self._cancelled()
        conn.channel.return_value = MagicMock(is_open=True)
        channel.basic_consume.side_effect = pika.exceptions.DuplicateConsumerTag("t")
        transport._consumer_supervisor.check_once()
        assert transport.cancelled_consumers == frozenset()

    def test_a_channel_refusal_is_logged_and_retried(self, caplog: pytest.LogCaptureFixture) -> None:
        transport, conn, channel, _tag = self._cancelled()
        conn.channel.return_value = MagicMock(is_open=True)
        channel.basic_consume.side_effect = pika.exceptions.ChannelClosedByBroker(403, "ACCESS_REFUSED")
        with caplog.at_level("WARNING", logger="rabbitkit.sync.transport"):
            transport._consumer_supervisor.check_once()
        assert "Re-subscribing to queue 'orders' failed" in caplog.text
        assert transport.cancelled_consumers == frozenset({"orders"})

    def test_a_probe_refusal_other_than_404_is_logged(self, caplog: pytest.LogCaptureFixture) -> None:
        transport, conn, _channel, _tag = self._cancelled()
        probe = MagicMock()
        probe.is_open = False
        probe.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(403, "ACCESS_REFUSED")
        conn.channel.return_value = probe
        with caplog.at_level("WARNING", logger="rabbitkit.sync.transport"):
            transport._consumer_supervisor.check_once()
        assert "Probing queue 'orders' for re-subscription failed" in caplog.text

    def test_no_probe_on_a_dead_connection(self) -> None:
        transport, conn, _channel, _tag = self._cancelled()
        conn.is_open = False
        calls = conn.channel.call_count
        assert transport._queue_exists_now("orders") is False
        assert conn.channel.call_count == calls

    def test_the_consume_loop_supervises_on_its_own_thread(self) -> None:
        transport, conn, _channel, _tag = self._cancelled()
        transport.poll_interval = 0.25
        seen: list[tuple[float, int]] = []

        def tick(time_limit: float) -> None:
            seen.append((time_limit, threading.get_ident()))
            transport._consumer_channels.clear()  # one iteration

        conn.process_data_events.side_effect = tick
        with patch.object(transport._consumer_supervisor, "check_once") as check:
            transport.start_consuming()
        check.assert_called_once_with()
        assert seen == [(0.25, threading.get_ident())]


class TestCancelAndDisconnect:
    def test_cancelling_a_broker_cancelled_consumer_sends_no_basic_cancel(self) -> None:
        transport, _conn, channel = _connected()
        tag = transport.consume("orders", lambda m: None)
        _cancel(transport, channel, tag)
        transport.cancel_consumer(tag)
        channel.basic_cancel.assert_not_called()
        assert transport._consumer_supervisor.record("orders") is None

    def test_cancelling_a_live_consumer_forgets_its_record(self) -> None:
        transport, _conn, channel = _connected()
        tag = transport.consume("orders", lambda m: None)
        transport.cancel_consumer(tag)
        channel.basic_cancel.assert_called_once_with(consumer_tag=tag)
        assert transport._consumer_supervisor.record("orders") is None

    def test_disconnect_and_a_poisoned_connection_forget_every_record(self) -> None:
        transport, _conn, channel = _connected()
        tag = transport.consume("orders", lambda m: None)
        _cancel(transport, channel, tag)
        transport.disconnect()
        assert transport.cancelled_consumers == frozenset()
        transport2, _conn2, _channel2 = _connected()
        transport2.consume("orders", lambda m: None)
        transport2._poison_wedged_connection()
        assert transport2._consumer_supervisor.record("orders") is None


class TestPublishesInFlight:
    def test_counted_while_publishing_and_released_after(self) -> None:
        transport = SyncTransport(connection_config=ConnectionConfig())
        seen: list[int] = []

        def fake_publish(envelope: MessageEnvelope) -> PublishOutcome:
            seen.append(transport.publishes_in_flight)
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        with patch.object(transport, "_publish", side_effect=fake_publish):
            assert transport.publish(MessageEnvelope(routing_key="q", body=b"")).ok
        assert seen == [1]
        assert transport.publishes_in_flight == 0

    def test_released_when_the_publish_raises(self) -> None:
        transport = SyncTransport(connection_config=ConnectionConfig())
        with patch.object(transport, "_publish", side_effect=RuntimeError("bug")), pytest.raises(RuntimeError):
            transport.publish(MessageEnvelope(routing_key="q", body=b""))
        assert transport.publishes_in_flight == 0

    def test_concurrent_publishers_are_all_counted(self) -> None:
        transport = SyncTransport(connection_config=ConnectionConfig())
        gate = threading.Barrier(4)
        release = threading.Event()

        def fake_publish(envelope: MessageEnvelope) -> PublishOutcome:
            gate.wait(timeout=5)
            release.wait(timeout=5)
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        with patch.object(transport, "_publish", side_effect=fake_publish):
            threads = [
                threading.Thread(target=transport.publish, args=(MessageEnvelope(routing_key="q", body=b""),))
                for _ in range(3)
            ]
            for t in threads:
                t.start()
            gate.wait(timeout=5)
            assert transport.publishes_in_flight == 3
            release.set()
            for t in threads:
                t.join(timeout=5)
        assert transport.publishes_in_flight == 0
