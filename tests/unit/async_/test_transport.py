"""Tests for async_/transport.py — AsyncTransportImpl (mocked aio-pika)."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rabbitkit.async_.transport import AsyncTransportImpl
from rabbitkit.core.config import ConnectionConfig, SecurityConfig
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import MessageEnvelope, PublishStatus, TopologyMode

# ── helpers ───────────────────────────────────────────────────────────────


def _make_transport(**kwargs) -> AsyncTransportImpl:
    return AsyncTransportImpl(
        connection_config=ConnectionConfig(),
        security_config=SecurityConfig(),
        **kwargs,
    )


def _make_mock_connection() -> AsyncMock:
    """Return a mock aio_pika connection with a mock channel factory."""
    mock_channel = AsyncMock()
    mock_channel.is_closed = False
    mock_channel.set_qos = AsyncMock()
    mock_channel.declare_exchange = AsyncMock()
    mock_channel.declare_queue = AsyncMock()
    mock_channel.get_exchange = AsyncMock()
    mock_channel.get_queue = AsyncMock()
    mock_channel.default_exchange = AsyncMock()
    mock_channel.close = AsyncMock()

    mock_connection = AsyncMock()
    mock_connection.is_closed = False
    mock_connection.channel = AsyncMock(return_value=mock_channel)
    mock_connection.close = AsyncMock()
    return mock_connection


# ── Construction ─────────────────────────────────────────────────────────


class TestConstruction:
    def test_default_construction(self) -> None:
        transport = _make_transport()
        assert not transport.is_connected()

    def test_lazy_connect(self) -> None:
        """Transport does NOT connect in __init__."""
        transport = _make_transport()
        assert not transport._connected
        assert transport._topology_channel is None


# ── Connection (mocked) ─────────────────────────────────────────────────


class TestConnection:
    @pytest.mark.asyncio
    async def test_connect(self) -> None:
        transport = _make_transport()
        mock_connection = _make_mock_connection()

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()
                assert transport.is_connected()

    @pytest.mark.asyncio
    async def test_connect_without_confirms(self) -> None:
        """confirm_delivery=False is stored; connect still succeeds."""
        transport = _make_transport(confirm_delivery=False)
        mock_connection = _make_mock_connection()

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()
                assert transport.is_connected()

    @pytest.mark.asyncio
    async def test_disconnect(self) -> None:
        transport = _make_transport()
        mock_connection = _make_mock_connection()

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()
                assert transport.is_connected()

                await transport.disconnect()
                assert not transport.is_connected()

    @pytest.mark.asyncio
    async def test_connect_idempotent(self) -> None:
        transport = _make_transport()
        mock_connection = _make_mock_connection()

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection) as mock_connect:
                await transport.connect()
                await transport.connect()  # second call should be no-op
                # connect_robust called twice: once for publisher, once for consumer connection
                assert mock_connect.call_count == 2  # publisher + consumer connections


# ── Topology ─────────────────────────────────────────────────────────────


class TestTopology:
    async def _connect_transport(self, transport: AsyncTransportImpl) -> AsyncMock:
        """Helper to connect transport with mocked aio-pika.
        Returns the topology channel mock.
        """
        mock_connection = _make_mock_connection()
        topology_channel = mock_connection.channel.return_value

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()

        return topology_channel

    @pytest.mark.asyncio
    async def test_declare_exchange(self) -> None:
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        exchange = RabbitExchange(name="events")
        await transport.declare_exchange(exchange)

        channel.declare_exchange.assert_called_once()

    @pytest.mark.asyncio
    async def test_declare_exchange_manual_mode_skips(self) -> None:
        transport = _make_transport(topology_mode=TopologyMode.MANUAL)
        channel = await self._connect_transport(transport)

        exchange = RabbitExchange(name="events")
        await transport.declare_exchange(exchange)

        channel.declare_exchange.assert_not_called()
        channel.get_exchange.assert_not_called()

    @pytest.mark.asyncio
    async def test_declare_exchange_passive_mode(self) -> None:
        transport = _make_transport(topology_mode=TopologyMode.PASSIVE_ONLY)
        channel = await self._connect_transport(transport)

        exchange = RabbitExchange(name="events")
        await transport.declare_exchange(exchange)

        channel.get_exchange.assert_called_once()

    @pytest.mark.asyncio
    async def test_declare_queue(self) -> None:
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        queue = RabbitQueue(name="orders")
        await transport.declare_queue(queue)

        channel.declare_queue.assert_called_once()

    @pytest.mark.asyncio
    async def test_declare_queue_manual_mode_skips(self) -> None:
        transport = _make_transport(topology_mode=TopologyMode.MANUAL)
        channel = await self._connect_transport(transport)

        queue = RabbitQueue(name="orders")
        await transport.declare_queue(queue)

        channel.declare_queue.assert_not_called()

    @pytest.mark.asyncio
    async def test_declare_queue_passive_mode(self) -> None:
        transport = _make_transport(topology_mode=TopologyMode.PASSIVE_ONLY)
        channel = await self._connect_transport(transport)

        queue = RabbitQueue(name="orders")
        await transport.declare_queue(queue)

        channel.get_queue.assert_called_once()

    @pytest.mark.asyncio
    async def test_bind_queue(self) -> None:
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        mock_queue = AsyncMock()
        mock_exchange = AsyncMock()
        channel.get_queue = AsyncMock(return_value=mock_queue)
        channel.get_exchange = AsyncMock(return_value=mock_exchange)

        await transport.bind_queue("orders", "events", "orders.created")

        mock_queue.bind.assert_called_once_with(mock_exchange, routing_key="orders.created")

    @pytest.mark.asyncio
    async def test_bind_queue_manual_mode_skips(self) -> None:
        transport = _make_transport(topology_mode=TopologyMode.MANUAL)
        channel = await self._connect_transport(transport)

        await transport.bind_queue("orders", "events", "orders.created")

        channel.get_queue.assert_not_called()


# ── Publish ──────────────────────────────────────────────────────────────


class TestPublish:
    async def _connect_transport(self, transport: AsyncTransportImpl) -> AsyncMock:
        mock_connection = _make_mock_connection()
        publisher_channel = mock_connection.channel.return_value

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()

        return publisher_channel

    @pytest.mark.asyncio
    async def test_publish_success(self) -> None:
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        mock_exchange = AsyncMock()
        channel.get_exchange = AsyncMock(return_value=mock_exchange)

        envelope = MessageEnvelope(
            routing_key="orders.created",
            body=b'{"id": 1}',
            exchange="events",
        )

        with patch("aio_pika.Message") as mock_msg_cls:
            mock_msg_cls.return_value = MagicMock()
            outcome = await transport.publish(envelope)

        assert outcome.ok
        assert outcome.status == PublishStatus.CONFIRMED
        mock_exchange.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_default_exchange(self) -> None:
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        envelope = MessageEnvelope(
            routing_key="orders",
            body=b"hello",
            exchange="",  # default exchange
        )

        with patch("aio_pika.Message") as mock_msg_cls:
            mock_msg_cls.return_value = MagicMock()
            outcome = await transport.publish(envelope)

        assert outcome.ok
        # Should use default_exchange, not get_exchange
        channel.get_exchange.assert_not_called()

    @pytest.mark.asyncio
    async def test_publish_error(self) -> None:
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        mock_exchange = AsyncMock()
        mock_exchange.publish.side_effect = Exception("publish failed")
        channel.get_exchange = AsyncMock(return_value=mock_exchange)

        envelope = MessageEnvelope(
            routing_key="rk",
            body=b"hello",
            exchange="ex",
        )

        with patch("aio_pika.Message") as mock_msg_cls:
            mock_msg_cls.return_value = MagicMock()
            outcome = await transport.publish(envelope)

        assert not outcome.ok
        assert outcome.status == PublishStatus.ERROR


# ── Consume ──────────────────────────────────────────────────────────────


class TestConsume:
    async def _connect_transport(self, transport: AsyncTransportImpl) -> AsyncMock:
        mock_connection = _make_mock_connection()

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()

        return mock_connection.channel.return_value

    @pytest.mark.asyncio
    async def test_consume_returns_tag(self) -> None:
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        mock_queue = AsyncMock()
        channel.get_queue = AsyncMock(return_value=mock_queue)

        tag = await transport.consume("orders", AsyncMock(), prefetch=10)

        assert tag.startswith("rabbitkit.")

    @pytest.mark.asyncio
    async def test_consume_sets_prefetch(self) -> None:
        """consume() sets QoS per-consumer channel."""
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        mock_queue = AsyncMock()
        channel.get_queue = AsyncMock(return_value=mock_queue)

        await transport.consume("orders", AsyncMock(), prefetch=50)

        channel.set_qos.assert_called_with(prefetch_count=50)

    @pytest.mark.asyncio
    async def test_consume_passively_declares_queue_for_robust_restoration(self) -> None:
        """Regression: consume() must declare_queue(passive=True), NOT get_queue().

        RobustChannel only restores queues in its _queues registry (populated by
        declare_queue, not get_queue), so get_queue left the consumer un-restored
        after a reconnect — a broker bounce silently stopped consumption.
        """
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        mock_queue = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=mock_queue)
        channel.get_queue = AsyncMock(return_value=mock_queue)

        await transport.consume("orders", AsyncMock(), prefetch=10)

        channel.declare_queue.assert_awaited_once_with("orders", passive=True)
        channel.get_queue.assert_not_called()
        mock_queue.consume.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_cancel_consumer(self) -> None:
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        mock_queue = AsyncMock()
        channel.get_queue = AsyncMock(return_value=mock_queue)

        tag = await transport.consume("orders", AsyncMock())
        await transport.cancel_consumer(tag)

        # Should have called cancel on the queue
        assert mock_queue.cancel.called


# ── Message Building ────────────────────────────────────────────────────


class TestBuildMessage:
    def test_build_message_from_aio_pika(self) -> None:
        transport = _make_transport()

        mock_aio_msg = MagicMock()
        mock_aio_msg.body = b'{"test": true}'
        mock_aio_msg.headers = {"x-custom": "value"}
        mock_aio_msg.message_id = "msg-1"
        mock_aio_msg.correlation_id = "corr-1"
        mock_aio_msg.reply_to = "reply-queue"
        mock_aio_msg.content_type = "application/json"
        mock_aio_msg.content_encoding = None
        mock_aio_msg.type = None
        mock_aio_msg.app_id = "test-app"
        mock_aio_msg.routing_key = "orders.created"
        mock_aio_msg.exchange = "events"
        mock_aio_msg.delivery_tag = 42
        mock_aio_msg.redelivered = False
        mock_aio_msg.consumer_tag = "tag-1"

        message = transport._build_message(mock_aio_msg)

        assert message.body == b'{"test": true}'
        assert message.headers == {"x-custom": "value"}
        assert message.message_id == "msg-1"
        assert message.correlation_id == "corr-1"
        assert message.reply_to == "reply-queue"
        assert message.routing_key == "orders.created"
        assert message.exchange == "events"
        assert message.delivery_tag == 42
        assert message.redelivered is False

    def test_build_message_wires_async_settlement(self) -> None:
        transport = _make_transport()

        mock_aio_msg = MagicMock()
        mock_aio_msg.body = b"test"
        mock_aio_msg.headers = {}
        mock_aio_msg.message_id = None
        mock_aio_msg.correlation_id = None
        mock_aio_msg.reply_to = None
        mock_aio_msg.content_type = None
        mock_aio_msg.content_encoding = None
        mock_aio_msg.type = None
        mock_aio_msg.app_id = None
        mock_aio_msg.routing_key = "rk"
        mock_aio_msg.exchange = ""
        mock_aio_msg.delivery_tag = 1
        mock_aio_msg.redelivered = False
        mock_aio_msg.consumer_tag = "tag"

        message = transport._build_message(mock_aio_msg)

        # Should have async settlement functions wired
        assert message._ack_async_fn is not None
        assert message._nack_async_fn is not None
        assert message._reject_async_fn is not None

    def test_build_message_empty_headers(self) -> None:
        transport = _make_transport()

        mock_aio_msg = MagicMock()
        mock_aio_msg.body = b"test"
        mock_aio_msg.headers = None
        mock_aio_msg.message_id = None
        mock_aio_msg.correlation_id = None
        mock_aio_msg.reply_to = None
        mock_aio_msg.content_type = None
        mock_aio_msg.content_encoding = None
        mock_aio_msg.type = None
        mock_aio_msg.app_id = None
        mock_aio_msg.routing_key = "rk"
        mock_aio_msg.exchange = ""
        mock_aio_msg.delivery_tag = 1
        mock_aio_msg.redelivered = False
        mock_aio_msg.consumer_tag = "tag"

        message = transport._build_message(mock_aio_msg)

        assert message.headers == {}

    async def test_ack_fn_calls_aio_message_ack(self) -> None:
        """Lines 335: ack_fn() calls aio_message.ack()."""
        transport = _make_transport()
        mock_aio_msg = MagicMock()
        mock_aio_msg.body = b"test"
        mock_aio_msg.headers = {}
        mock_aio_msg.message_id = None
        mock_aio_msg.correlation_id = None
        mock_aio_msg.reply_to = None
        mock_aio_msg.content_type = None
        mock_aio_msg.content_encoding = None
        mock_aio_msg.type = None
        mock_aio_msg.app_id = None
        mock_aio_msg.routing_key = "rk"
        mock_aio_msg.exchange = ""
        mock_aio_msg.delivery_tag = 1
        mock_aio_msg.redelivered = False
        mock_aio_msg.consumer_tag = "tag"
        mock_aio_msg.ack = AsyncMock()

        message = transport._build_message(mock_aio_msg)
        await message._ack_async_fn()

        mock_aio_msg.ack.assert_called_once()

    async def test_nack_fn_calls_aio_message_nack(self) -> None:
        """Lines 338: nack_fn() calls aio_message.nack()."""
        transport = _make_transport()
        mock_aio_msg = MagicMock()
        mock_aio_msg.body = b"test"
        mock_aio_msg.headers = {}
        mock_aio_msg.message_id = None
        mock_aio_msg.correlation_id = None
        mock_aio_msg.reply_to = None
        mock_aio_msg.content_type = None
        mock_aio_msg.content_encoding = None
        mock_aio_msg.type = None
        mock_aio_msg.app_id = None
        mock_aio_msg.routing_key = "rk"
        mock_aio_msg.exchange = ""
        mock_aio_msg.delivery_tag = 1
        mock_aio_msg.redelivered = False
        mock_aio_msg.consumer_tag = "tag"
        mock_aio_msg.nack = AsyncMock()

        message = transport._build_message(mock_aio_msg)
        await message._nack_async_fn(requeue=False)

        mock_aio_msg.nack.assert_called_once_with(requeue=False)

    async def test_reject_fn_calls_aio_message_reject(self) -> None:
        """Lines 341: reject_fn() calls aio_message.reject()."""
        transport = _make_transport()
        mock_aio_msg = MagicMock()
        mock_aio_msg.body = b"test"
        mock_aio_msg.headers = {}
        mock_aio_msg.message_id = None
        mock_aio_msg.correlation_id = None
        mock_aio_msg.reply_to = None
        mock_aio_msg.content_type = None
        mock_aio_msg.content_encoding = None
        mock_aio_msg.type = None
        mock_aio_msg.app_id = None
        mock_aio_msg.routing_key = "rk"
        mock_aio_msg.exchange = ""
        mock_aio_msg.delivery_tag = 1
        mock_aio_msg.redelivered = False
        mock_aio_msg.consumer_tag = "tag"
        mock_aio_msg.reject = AsyncMock()

        message = transport._build_message(mock_aio_msg)
        await message._reject_async_fn(requeue=True)

        mock_aio_msg.reject.assert_called_once_with(requeue=True)


# ── Disconnect edge cases ────────────────────────────────────────────────


class TestDisconnectEdgeCases:
    async def _connected_transport(self) -> AsyncTransportImpl:
        """Return a fully connected transport with mocked aio-pika."""
        transport = _make_transport()
        mock_connection = _make_mock_connection()
        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()
        return transport

    async def test_disconnect_when_not_connected_is_noop(self) -> None:
        """Line 93: disconnect() returns immediately when not connected."""
        transport = _make_transport()
        assert not transport.is_connected()
        # Should not raise or hang
        await transport.disconnect()
        assert not transport.is_connected()

    async def test_disconnect_with_closed_consumer_channel(self) -> None:
        """Lines 98-102: consumer channel that is already closed is skipped."""
        transport = await self._connected_transport()

        # Inject a closed consumer channel
        closed_ch = MagicMock()
        closed_ch.is_closed = True
        closed_ch.close = AsyncMock()
        transport._consumer_channels["q1"] = closed_ch

        await transport.disconnect()

        # close() must NOT have been called because is_closed == True
        closed_ch.close.assert_not_called()
        assert not transport.is_connected()

    async def test_disconnect_consumer_channel_close_exception_is_swallowed(self) -> None:
        """Lines 98-102: exception from ch.close() is silently swallowed."""
        transport = await self._connected_transport()

        bad_ch = MagicMock()
        bad_ch.is_closed = False
        bad_ch.close = AsyncMock(side_effect=RuntimeError("channel gone"))
        transport._consumer_channels["q1"] = bad_ch

        # Must not raise
        await transport.disconnect()
        assert not transport.is_connected()

    async def test_disconnect_topology_channel_close_exception_is_swallowed(self) -> None:
        """Lines 110-111: exception from topology_channel.close() is swallowed."""
        transport = await self._connected_transport()

        bad_topo = MagicMock()
        bad_topo.is_closed = False
        bad_topo.close = AsyncMock(side_effect=OSError("topo gone"))
        transport._topology_channel = bad_topo

        # Must not raise
        await transport.disconnect()
        assert not transport.is_connected()

    async def test_disconnect_outer_exception_is_logged(self) -> None:
        """Lines 115-116: exception from close_all() is caught by outer handler."""
        transport = await self._connected_transport()

        with patch.object(
            transport._conn_pool, "close_all",
            new_callable=AsyncMock, side_effect=RuntimeError("pool dead"),
        ):
            # Must not raise even though close_all explodes
            await transport.disconnect()

        assert not transport.is_connected()


# ── _ensure_connected ────────────────────────────────────────────────────


class TestEnsureConnected:
    async def test_ensure_connected_triggers_connect_when_disconnected(self) -> None:
        """Line 129: _ensure_connected() calls connect() when not yet connected."""
        transport = _make_transport()
        mock_connection = _make_mock_connection()

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                assert not transport.is_connected()
                await transport._ensure_connected()
                assert transport.is_connected()


# ── on_message callback (consume) ────────────────────────────────────────


class TestOnMessageCallback:
    async def _connect_transport(self, transport: AsyncTransportImpl) -> AsyncMock:
        mock_connection = _make_mock_connection()
        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()
        return mock_connection.channel.return_value

    async def test_on_message_callback_invoked(self) -> None:
        """Lines 212-213: on_message inner callback builds message and calls handler."""
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        received: list = []

        async def handler(msg) -> None:
            received.append(msg)

        mock_queue = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=mock_queue)

        await transport.consume("orders", handler)

        # Retrieve the on_message callable registered with q.consume
        on_message_fn = mock_queue.consume.call_args[0][0]

        # Build a fake aio-pika message
        fake_aio_msg = MagicMock()
        fake_aio_msg.body = b"payload"
        fake_aio_msg.headers = {}
        fake_aio_msg.message_id = "m1"
        fake_aio_msg.correlation_id = None
        fake_aio_msg.reply_to = None
        fake_aio_msg.content_type = None
        fake_aio_msg.content_encoding = None
        fake_aio_msg.type = None
        fake_aio_msg.app_id = None
        fake_aio_msg.routing_key = "orders"
        fake_aio_msg.exchange = ""
        fake_aio_msg.delivery_tag = 1
        fake_aio_msg.redelivered = False
        fake_aio_msg.consumer_tag = "tag"

        await on_message_fn(fake_aio_msg)

        assert len(received) == 1
        assert received[0].body == b"payload"


# ── bind_exchange ────────────────────────────────────────────────────────


class TestBindExchange:
    async def _connect_transport(self, transport: AsyncTransportImpl) -> AsyncMock:
        mock_connection = _make_mock_connection()
        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()
        return mock_connection.channel.return_value

    async def test_bind_exchange_manual_mode_skips(self) -> None:
        """Line 283: bind_exchange() returns immediately in MANUAL mode."""
        transport = _make_transport(topology_mode=TopologyMode.MANUAL)
        channel = await self._connect_transport(transport)

        await transport.bind_exchange("dest", "src", "rk")

        channel.get_exchange.assert_not_called()

    async def test_bind_exchange_calls_dest_bind(self) -> None:
        """Lines 283-291: bind_exchange() gets both exchanges and calls dest.bind()."""
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        dest_mock = AsyncMock()
        src_mock = AsyncMock()
        channel.get_exchange = AsyncMock(side_effect=[dest_mock, src_mock])

        await transport.bind_exchange("dest_ex", "src_ex", routing_key="fanout.#", arguments={"x-arg": 1})

        dest_mock.bind.assert_called_once_with(src_mock, routing_key="fanout.#", arguments={"x-arg": 1})

    async def test_bind_exchange_default_routing_key(self) -> None:
        """bind_exchange() with default empty routing_key and no arguments."""
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        dest_mock = AsyncMock()
        src_mock = AsyncMock()
        channel.get_exchange = AsyncMock(side_effect=[dest_mock, src_mock])

        await transport.bind_exchange("dest_ex", "src_ex")

        dest_mock.bind.assert_called_once_with(src_mock, routing_key="", arguments=None)


# ── cancel_consumer edge cases ────────────────────────────────────────────


class TestCancelConsumerEdgeCases:
    async def _connect_transport(self, transport: AsyncTransportImpl) -> AsyncMock:
        mock_connection = _make_mock_connection()
        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()
        return mock_connection.channel.return_value

    async def test_cancel_consumer_when_not_connected_is_noop(self) -> None:
        """Line 296: cancel_consumer() returns immediately when not connected."""
        transport = _make_transport()
        assert not transport.is_connected()
        # Should not raise
        await transport.cancel_consumer("rabbitkit.some-tag")

    async def test_cancel_consumer_exception_is_logged(self) -> None:
        """Lines 305-306: exception from q.cancel() is caught and logged."""
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        mock_queue = AsyncMock()
        mock_queue.cancel = AsyncMock(side_effect=RuntimeError("cancel failed"))
        channel.get_queue = AsyncMock(return_value=mock_queue)

        tag = await transport.consume("orders", AsyncMock())

        # Must not raise even though q.cancel() explodes
        await transport.cancel_consumer(tag)

        # After the finally block the tracking dicts should be cleaned up
        assert "orders" not in transport._consumer_tags
        assert "orders" not in transport._consumer_channels
