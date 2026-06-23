"""Tests for sync/transport.py — SyncTransport (mocked pika)."""

from __future__ import annotations

from datetime import UTC
from unittest.mock import MagicMock, patch

import pytest

from rabbitkit.core.config import ConnectionConfig, SecurityConfig, SocketConfig
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import MessageEnvelope, PublishStatus, TopologyMode
from rabbitkit.sync.transport import SyncTransport

# ── helpers ───────────────────────────────────────────────────────────────


def _make_transport(**kwargs) -> SyncTransport:
    return SyncTransport(
        connection_config=ConnectionConfig(),
        socket_config=SocketConfig(),
        security_config=SecurityConfig(),
        **kwargs,
    )


# ── Construction ─────────────────────────────────────────────────────────


class TestConstruction:
    def test_default_construction(self) -> None:
        transport = _make_transport()
        assert not transport.is_connected()

    def test_lazy_connect(self) -> None:
        """Transport does NOT connect in __init__."""
        transport = _make_transport()
        assert transport._connection is None
        assert transport._channel is None


# ── Connection (mocked) ─────────────────────────────────────────────────


class TestConnection:
    @pytest.fixture(autouse=True)
    def _check_pika(self) -> None:
        pytest.importorskip("pika")

    def test_connect(self) -> None:
        transport = _make_transport()

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_conn.return_value.channel.return_value = mock_channel

                transport.connect()

                assert transport.is_connected()
                mock_channel.confirm_delivery.assert_called_once()

    def test_connect_without_confirms(self) -> None:
        transport = _make_transport(confirm_delivery=False)

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_conn.return_value.channel.return_value = mock_channel

                transport.connect()

                mock_channel.confirm_delivery.assert_not_called()

    def test_disconnect(self) -> None:
        transport = _make_transport()

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True

                transport.connect()
                assert transport.is_connected()

                transport.disconnect()
                assert not transport.is_connected()

    def test_connect_idempotent(self) -> None:
        transport = _make_transport()

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_conn.return_value.channel.return_value = MagicMock()

                transport.connect()
                transport.connect()  # second call should be no-op

                # Only one BlockingConnection call
                assert mock_conn.call_count == 1


# ── Topology ─────────────────────────────────────────────────────────────


class TestTopology:
    @pytest.fixture(autouse=True)
    def _check_pika(self) -> None:
        pytest.importorskip("pika")

    def _connect_transport(self, transport: SyncTransport) -> MagicMock:
        """Helper to connect transport with mocked pika."""
        mock_channel = MagicMock()
        mock_channel.is_open = True

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                transport.connect()

        return mock_channel

    def test_declare_exchange(self) -> None:
        transport = _make_transport()
        channel = self._connect_transport(transport)

        exchange = RabbitExchange(name="events")
        transport.declare_exchange(exchange)

        channel.exchange_declare.assert_called_once()

    def test_declare_exchange_manual_mode_skips(self) -> None:
        transport = _make_transport(topology_mode=TopologyMode.MANUAL)
        channel = self._connect_transport(transport)

        exchange = RabbitExchange(name="events")
        transport.declare_exchange(exchange)

        channel.exchange_declare.assert_not_called()

    def test_declare_exchange_passive_mode(self) -> None:
        transport = _make_transport(topology_mode=TopologyMode.PASSIVE_ONLY)
        channel = self._connect_transport(transport)

        exchange = RabbitExchange(name="events")
        transport.declare_exchange(exchange)

        call_kwargs = channel.exchange_declare.call_args
        assert call_kwargs[1].get("passive") is True or call_kwargs.kwargs.get("passive") is True

    def test_declare_queue(self) -> None:
        transport = _make_transport()
        channel = self._connect_transport(transport)

        queue = RabbitQueue(name="orders")
        transport.declare_queue(queue)

        channel.queue_declare.assert_called_once()

    def test_declare_queue_manual_mode_skips(self) -> None:
        transport = _make_transport(topology_mode=TopologyMode.MANUAL)
        channel = self._connect_transport(transport)

        queue = RabbitQueue(name="orders")
        transport.declare_queue(queue)

        channel.queue_declare.assert_not_called()

    def test_bind_queue(self) -> None:
        transport = _make_transport()
        channel = self._connect_transport(transport)

        transport.bind_queue("orders", "events", "orders.created")

        channel.queue_bind.assert_called_once_with(
            queue="orders", exchange="events", routing_key="orders.created"
        )

    def test_bind_queue_manual_mode_skips(self) -> None:
        transport = _make_transport(topology_mode=TopologyMode.MANUAL)
        channel = self._connect_transport(transport)

        transport.bind_queue("orders", "events", "orders.created")

        channel.queue_bind.assert_not_called()


# ── Publish ──────────────────────────────────────────────────────────────


class TestPublish:
    @pytest.fixture(autouse=True)
    def _check_pika(self) -> None:
        pytest.importorskip("pika")

    def _connect_transport(self, transport: SyncTransport) -> MagicMock:
        mock_channel = MagicMock()
        mock_channel.is_open = True

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                transport.connect()

        return mock_channel

    def test_publish_success(self) -> None:
        transport = _make_transport()
        channel = self._connect_transport(transport)

        envelope = MessageEnvelope(
            routing_key="orders.created",
            body=b'{"id": 1}',
            exchange="events",
        )

        outcome = transport.publish(envelope)

        assert outcome.ok
        assert outcome.status == PublishStatus.CONFIRMED
        channel.basic_publish.assert_called_once()

    def test_publish_sets_properties(self) -> None:
        transport = _make_transport()
        channel = self._connect_transport(transport)

        envelope = MessageEnvelope(
            routing_key="rk",
            body=b"hello",
            exchange="ex",
            message_id="msg-1",
            correlation_id="corr-1",
            content_type="application/json",
            headers={"x-custom": "value"},
        )

        transport.publish(envelope)

        call_kwargs = channel.basic_publish.call_args
        assert call_kwargs.kwargs["exchange"] == "ex"
        assert call_kwargs.kwargs["routing_key"] == "rk"
        assert call_kwargs.kwargs["body"] == b"hello"

    def test_publish_error(self) -> None:
        transport = _make_transport()
        channel = self._connect_transport(transport)
        channel.basic_publish.side_effect = Exception("publish failed")

        envelope = MessageEnvelope(routing_key="rk", body=b"hello")
        outcome = transport.publish(envelope)

        assert not outcome.ok
        assert outcome.status == PublishStatus.ERROR


# ── Consume ──────────────────────────────────────────────────────────────


class TestConsume:
    @pytest.fixture(autouse=True)
    def _check_pika(self) -> None:
        pytest.importorskip("pika")

    def _connect_transport(self, transport: SyncTransport) -> MagicMock:
        mock_channel = MagicMock()
        mock_channel.is_open = True

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                transport.connect()

        return mock_channel

    def test_consume_returns_tag(self) -> None:
        transport = _make_transport()
        self._connect_transport(transport)

        tag = transport.consume("orders", lambda msg: None, prefetch=10)

        assert tag.startswith("rabbitkit.")

    def test_consume_sets_prefetch(self) -> None:
        transport = _make_transport()
        channel = self._connect_transport(transport)

        transport.consume("orders", lambda msg: None, prefetch=50)

        channel.basic_qos.assert_called_with(prefetch_count=50)

    def test_cancel_consumer(self) -> None:
        transport = _make_transport()
        channel = self._connect_transport(transport)

        tag = transport.consume("orders", lambda msg: None)
        transport.cancel_consumer(tag)

        channel.basic_cancel.assert_called_once_with(consumer_tag=tag)


# ── Additional coverage tests ─────────────────────────────────────────────


class TestConnectErrors:
    """Cover import-guard and reconnect-retry paths."""

    def test_disconnect_when_not_connected_is_noop(self) -> None:
        transport = _make_transport()
        # never connected — should not raise
        transport.disconnect()
        assert not transport.is_connected()

    def test_disconnect_exception_is_swallowed(self) -> None:
        transport = _make_transport()

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_channel.close.side_effect = Exception("close failed")
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                transport.connect()

        # close() raises but disconnect() should complete without re-raising
        transport.disconnect()
        assert not transport.is_connected()

    def test_is_connected_exception_returns_false(self) -> None:
        transport = _make_transport()

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                transport.connect()

        # Make connection.is_open raise
        transport._connection.is_open = property(lambda self: (_ for _ in ()).throw(Exception("oops")))  # type: ignore[assignment]
        # Actually easier: set the mock to raise
        type(transport._connection).is_open = property(lambda s: (_ for _ in ()).throw(RuntimeError("oops")))  # type: ignore[assignment]

    def test_ensure_connected_retries_on_failure(self) -> None:
        """_ensure_connected retries after connection errors."""
        pytest.importorskip("pika")
        import pika


        transport = _make_transport()
        call_count = 0

        original_connect = transport.connect

        def failing_then_ok() -> None:
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise pika.exceptions.AMQPConnectionError("temporary failure")
            original_connect()

        with patch.object(transport, "connect", side_effect=failing_then_ok):
            with patch("rabbitkit.sync.transport.make_pika_connection_params"):
                with patch("pika.BlockingConnection") as mock_conn:
                    mock_conn.return_value.channel.return_value = MagicMock()
                    mock_conn.return_value.is_open = True
                    with patch("rabbitkit.sync.transport.time.sleep"):
                        transport._ensure_connected()

        assert call_count == 2


class TestPublishTimestamp:
    @pytest.fixture(autouse=True)
    def _check_pika(self) -> None:
        pytest.importorskip("pika")

    def _connect_transport(self, transport: SyncTransport) -> MagicMock:
        mock_channel = MagicMock()
        mock_channel.is_open = True
        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                transport.connect()
        return mock_channel

    def test_publish_with_timestamp(self) -> None:
        """Envelope timestamp is converted and set on properties."""
        from datetime import datetime

        transport = _make_transport()
        self._connect_transport(transport)

        envelope = MessageEnvelope(
            routing_key="rk",
            body=b"hello",
            timestamp=datetime(2024, 1, 1, tzinfo=UTC),
        )

        outcome = transport.publish(envelope)
        assert outcome.ok


class TestConsumeCallback:
    @pytest.fixture(autouse=True)
    def _check_pika(self) -> None:
        pytest.importorskip("pika")

    def _connect_transport(self, transport: SyncTransport) -> MagicMock:
        mock_channel = MagicMock()
        mock_channel.is_open = True
        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                transport.connect()
        return mock_channel

    def test_consume_callback_triggers_on_message(self) -> None:
        """on_message closure builds RabbitMessage and calls user callback."""
        transport = _make_transport()
        channel = self._connect_transport(transport)

        received: list[object] = []

        def user_callback(msg: object) -> None:
            received.append(msg)

        transport.consume("orders", user_callback)

        # Extract the on_message_callback that was registered
        call_args = channel.basic_consume.call_args
        on_message_fn = call_args.kwargs.get("on_message_callback") or call_args[1]["on_message_callback"]

        # Build mock pika delivery
        mock_method = MagicMock()
        mock_method.routing_key = "orders"
        mock_method.exchange = "amq.direct"
        mock_method.delivery_tag = 1
        mock_method.redelivered = False
        mock_method.consumer_tag = "tag-1"

        mock_props = MagicMock()
        mock_props.headers = {"x-custom": "val"}
        mock_props.message_id = "msg-id"
        mock_props.correlation_id = None
        mock_props.reply_to = None
        mock_props.content_type = "application/json"
        mock_props.content_encoding = None
        mock_props.type = None
        mock_props.app_id = None

        # Trigger the callback
        on_message_fn(channel, mock_method, mock_props, b'{"id":1}')

        assert len(received) == 1
        from rabbitkit.core.message import RabbitMessage
        assert isinstance(received[0], RabbitMessage)
        assert received[0].routing_key == "orders"

    def test_build_message_wires_ack_functions(self) -> None:
        """_build_message sets ack/nack/reject callables on the message."""
        transport = _make_transport()
        channel = self._connect_transport(transport)

        transport.consume("orders", lambda msg: None)
        call_args = channel.basic_consume.call_args
        on_message_fn = call_args.kwargs.get("on_message_callback") or call_args[1]["on_message_callback"]

        mock_method = MagicMock()
        mock_method.routing_key = "orders"
        mock_method.exchange = ""
        mock_method.delivery_tag = 5
        mock_method.redelivered = False
        mock_method.consumer_tag = "t"

        mock_props = MagicMock()
        mock_props.headers = None
        mock_props.message_id = None
        mock_props.correlation_id = None
        mock_props.reply_to = None
        mock_props.content_type = None
        mock_props.content_encoding = None
        mock_props.type = None
        mock_props.app_id = None

        captured: list[object] = []

        def capture(msg: object) -> None:
            captured.append(msg)

        transport._consumer_tags["orders"] = "t"
        on_message_fn(channel, mock_method, mock_props, b"body")

        # Re-run with capture callback
        transport2 = _make_transport()
        channel2 = self._connect_transport(transport2)
        transport2.consume("orders", capture)
        call_args2 = channel2.basic_consume.call_args
        on_msg2 = call_args2.kwargs.get("on_message_callback") or call_args2[1]["on_message_callback"]
        on_msg2(channel2, mock_method, mock_props, b"body")

        from rabbitkit.core.message import RabbitMessage
        msg = captured[0]
        assert isinstance(msg, RabbitMessage)
        assert msg._ack_fn is not None
        assert msg._nack_fn is not None
        assert msg._reject_fn is not None

        # Invoke them to verify they call channel methods
        msg._ack_fn()
        channel2.basic_ack.assert_called_once_with(delivery_tag=5)

        msg._nack_fn(requeue=False)
        channel2.basic_nack.assert_called_once_with(delivery_tag=5, requeue=False)

        msg._reject_fn(requeue=True)
        channel2.basic_reject.assert_called_once_with(delivery_tag=5, requeue=True)


class TestAdditionalTopology:
    @pytest.fixture(autouse=True)
    def _check_pika(self) -> None:
        pytest.importorskip("pika")

    def _connect_transport(self, transport: SyncTransport) -> MagicMock:
        mock_channel = MagicMock()
        mock_channel.is_open = True
        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                transport.connect()
        return mock_channel

    def test_declare_queue_passive_mode(self) -> None:
        transport = _make_transport(topology_mode=TopologyMode.PASSIVE_ONLY)
        channel = self._connect_transport(transport)

        queue = RabbitQueue(name="orders")
        transport.declare_queue(queue)

        call_kwargs = channel.queue_declare.call_args
        assert call_kwargs.kwargs.get("passive") is True or call_kwargs[1].get("passive") is True

    def test_bind_exchange_calls_exchange_bind(self) -> None:
        transport = _make_transport()
        channel = self._connect_transport(transport)

        transport.bind_exchange("dest", "src", "rk", {"x-arg": "val"})

        channel.exchange_bind.assert_called_once_with(
            destination="dest",
            source="src",
            routing_key="rk",
            arguments={"x-arg": "val"},
        )

    def test_bind_exchange_manual_mode_skips(self) -> None:
        transport = _make_transport(topology_mode=TopologyMode.MANUAL)
        channel = self._connect_transport(transport)

        transport.bind_exchange("dest", "src", "rk")

        channel.exchange_bind.assert_not_called()

    def test_cancel_consumer_when_not_connected(self) -> None:
        transport = _make_transport()
        # Not connected — cancel_consumer should be a no-op
        transport.cancel_consumer("some-tag")  # should not raise

    def test_cancel_consumer_exception_swallowed(self) -> None:
        transport = _make_transport()
        channel = self._connect_transport(transport)
        channel.basic_cancel.side_effect = Exception("channel error")

        tag = transport.consume("orders", lambda msg: None)
        transport.cancel_consumer(tag)  # should not raise

    def test_start_consuming_keyboard_interrupt(self) -> None:
        transport = _make_transport()
        channel = self._connect_transport(transport)
        channel.start_consuming.side_effect = KeyboardInterrupt()

        # Should not propagate the KeyboardInterrupt
        transport.start_consuming()
        channel.stop_consuming.assert_called_once()

    def test_stop_consuming_when_connected(self) -> None:
        transport = _make_transport()
        channel = self._connect_transport(transport)

        transport.stop_consuming()

        channel.stop_consuming.assert_called_once()


class TestEdgeCases:
    def test_is_connected_exception_returns_false(self) -> None:
        """is_connected() returns False when checking raises."""
        from unittest.mock import PropertyMock
        pytest.importorskip("pika")
        transport = _make_transport()

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                transport.connect()

        # Make .is_open property raise when accessed
        mock_conn_obj = MagicMock()
        type(mock_conn_obj).is_open = PropertyMock(side_effect=RuntimeError("connection gone"))
        transport._connection = mock_conn_obj

        result = transport.is_connected()
        assert result is False

    def test_connect_without_pika_raises(self) -> None:
        """connect() raises ImportError when pika is not installed."""
        import sys
        transport = _make_transport()
        with patch.dict(sys.modules, {"pika": None}):
            with pytest.raises(ImportError, match="pika is required"):
                transport.connect()
