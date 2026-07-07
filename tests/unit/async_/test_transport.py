"""Tests for async_/transport.py — AsyncTransportImpl (mocked aio-pika)."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from rabbitkit.async_.transport import AsyncTransportImpl
from rabbitkit.core.config import ConnectionConfig, SecurityConfig
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus, TopologyMode

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
    async def test_declare_queue_precondition_failed_raises_configuration_error(self) -> None:
        """M6: a 406 PRECONDITION_FAILED (e.g. an ops-created queue with
        different arguments) must raise a typed ConfigurationError naming
        the conflicting queue -- not an opaque aio-pika channel-closed error."""
        import aio_pika.exceptions

        from rabbitkit.core.errors import ConfigurationError

        transport = _make_transport()
        channel = await self._connect_transport(transport)
        channel.declare_queue.side_effect = aio_pika.exceptions.ChannelPreconditionFailed(
            406, "PRECONDITION_FAILED - inequivalent arg 'x-queue-type' for queue 'orders'"
        )

        queue = RabbitQueue(name="orders")
        with pytest.raises(ConfigurationError, match="orders") as exc_info:
            await transport.declare_queue(queue)

        assert "PRECONDITION_FAILED" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, aio_pika.exceptions.ChannelPreconditionFailed)

    @pytest.mark.asyncio
    async def test_declare_queue_406_warn_continue(self) -> None:
        """M14: on_topology_conflict='warn_continue' warns and continues with
        the existing definition, reopening the topology channel."""
        import aio_pika.exceptions

        transport = _make_transport(on_topology_conflict="warn_continue")
        channel = await self._connect_transport(transport)
        channel.declare_queue.side_effect = aio_pika.exceptions.ChannelPreconditionFailed(
            406, "PRECONDITION_FAILED - inequivalent arg 'x-queue-type' for queue 'orders'"
        )
        # Make the reopen return a distinct channel so we can assert it happened.
        reopened = AsyncMock()
        new_conn = AsyncMock()
        new_conn.channel = AsyncMock(return_value=reopened)
        transport._conn_pool.get_consumer_connection = AsyncMock(return_value=new_conn)

        await transport.declare_queue(RabbitQueue(name="orders"))  # must NOT raise

        assert transport._topology_channel is reopened  # reopened for further declares

    @pytest.mark.asyncio
    async def test_declare_exchange_precondition_failed_raises_configuration_error(self) -> None:
        """M6: same as the queue case, for exchange declaration."""
        import aio_pika.exceptions

        from rabbitkit.core.errors import ConfigurationError

        transport = _make_transport()
        channel = await self._connect_transport(transport)
        channel.declare_exchange.side_effect = aio_pika.exceptions.ChannelPreconditionFailed(
            406, "PRECONDITION_FAILED - inequivalent arg 'type' for exchange 'events'"
        )

        exchange = RabbitExchange(name="events")
        with pytest.raises(ConfigurationError, match="events"):
            await transport.declare_exchange(exchange)

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

        mock_queue.bind.assert_called_once_with(mock_exchange, routing_key="orders.created", arguments=None)

    async def test_bind_queue_passes_arguments(self) -> None:
        """C4: headers-exchange match criteria must reach the broker."""
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        mock_queue = AsyncMock()
        mock_exchange = AsyncMock()
        channel.get_queue = AsyncMock(return_value=mock_queue)
        channel.get_exchange = AsyncMock(return_value=mock_exchange)

        args = {"x-match": "any", "type": "order"}
        await transport.bind_queue("orders.headers", "events.headers", "", arguments=args)

        mock_queue.bind.assert_called_once_with(mock_exchange, routing_key="", arguments=args)

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
    async def test_publish_expiration_ms_converted_to_seconds(self) -> None:
        """Regression: envelope.expiration is milliseconds; aio-pika expects SECONDS.
        Was `* 1000`, making the TTL 1e6x too long and inconsistent with sync."""
        transport = _make_transport()
        channel = await self._connect_transport(transport)
        channel.get_exchange = AsyncMock(return_value=AsyncMock())

        envelope = MessageEnvelope(routing_key="q", body=b"{}", exchange="e", expiration="60000")
        with patch("aio_pika.Message") as mock_msg_cls:
            mock_msg_cls.return_value = MagicMock()
            await transport.publish(envelope)

        assert mock_msg_cls.call_args.kwargs["expiration"] == 60.0  # 60000 ms -> 60 s

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

    @pytest.mark.asyncio
    async def test_publish_mandatory_uses_dedicated_confirm_channel(self) -> None:
        """H1: mandatory=True routes through _get_mandatory_channel() — always
        publisher_confirms=True + on_return_raises=True — regardless of the
        transport's own confirm_delivery setting, so an unroutable return is
        reliably detectable."""
        transport = _make_transport(confirm_delivery=False)
        channel = await self._connect_transport(transport)

        mock_exchange = AsyncMock()
        channel.get_exchange = AsyncMock(return_value=mock_exchange)

        envelope = MessageEnvelope(routing_key="rk", body=b"hello", exchange="ex", mandatory=True)

        with patch("aio_pika.Message") as mock_msg_cls:
            mock_msg_cls.return_value = MagicMock()
            outcome = await transport.publish(envelope)

        assert outcome.ok
        mock_exchange.publish.assert_called_once()
        transport._conn_pool._publisher_connection.channel.assert_any_call(
            publisher_confirms=True, on_return_raises=True
        )

    @pytest.mark.asyncio
    async def test_publish_unroutable_mandatory_returns_returned_status(self) -> None:
        """H1: aio_pika.exceptions.PublishError (broker returned the message —
        no matching binding) must map to PublishStatus.RETURNED, not the
        generic ERROR."""
        import aio_pika.exceptions

        transport = _make_transport()
        channel = await self._connect_transport(transport)

        mock_exchange = AsyncMock()
        mock_exchange.publish.side_effect = aio_pika.exceptions.PublishError.__new__(
            aio_pika.exceptions.PublishError
        )
        channel.get_exchange = AsyncMock(return_value=mock_exchange)

        envelope = MessageEnvelope(routing_key="rk", body=b"hello", exchange="ex", mandatory=True)

        with patch("aio_pika.Message") as mock_msg_cls:
            mock_msg_cls.return_value = MagicMock()
            outcome = await transport.publish(envelope)

        assert not outcome.ok
        assert outcome.status == PublishStatus.RETURNED
        assert outcome.error is not None

    @pytest.mark.asyncio
    async def test_publish_nacked_by_broker_returns_nacked_status(self) -> None:
        """H1: a plain aio_pika.exceptions.DeliveryError (broker Basic.Nack,
        not a return) must map to PublishStatus.NACKED, not the generic ERROR."""
        import aio_pika.exceptions

        transport = _make_transport()
        channel = await self._connect_transport(transport)

        mock_exchange = AsyncMock()
        mock_exchange.publish.side_effect = aio_pika.exceptions.DeliveryError.__new__(
            aio_pika.exceptions.DeliveryError
        )
        channel.get_exchange = AsyncMock(return_value=mock_exchange)

        envelope = MessageEnvelope(routing_key="rk", body=b"hello", exchange="ex")

        with patch("aio_pika.Message") as mock_msg_cls:
            mock_msg_cls.return_value = MagicMock()
            outcome = await transport.publish(envelope)

        assert not outcome.ok
        assert outcome.status == PublishStatus.NACKED
        assert outcome.error is not None

    @pytest.mark.asyncio
    async def test_disconnect_closes_mandatory_publish_channel(self) -> None:
        """The dedicated mandatory-publish channel must be closed and reset
        on disconnect(), matching the fast-channel cleanup."""
        transport = _make_transport()
        channel = await self._connect_transport(transport)
        channel.get_exchange = AsyncMock(return_value=AsyncMock())

        envelope = MessageEnvelope(routing_key="rk", body=b"hello", exchange="ex", mandatory=True)
        with patch("aio_pika.Message") as mock_msg_cls:
            mock_msg_cls.return_value = MagicMock()
            await transport.publish(envelope)

        assert transport._mandatory_publish_channel is not None
        channel.is_closed = False

        await transport.disconnect()

        assert transport._mandatory_publish_channel is None
        channel.close.assert_called()


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

    @pytest.mark.asyncio
    async def test_consume_declare_false_uses_get_queue_not_declare(self) -> None:
        """C2: declare=False must skip declare_queue entirely and use get_queue
        (ensure=False) instead — required for amq.rabbitmq.reply-to, which the
        broker rejects any Queue.Declare (even passive) for."""
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        mock_queue = AsyncMock()
        channel.get_queue = AsyncMock(return_value=mock_queue)
        channel.declare_queue = AsyncMock()

        await transport.consume("amq.rabbitmq.reply-to", AsyncMock(), declare=False)

        channel.declare_queue.assert_not_called()
        channel.get_queue.assert_awaited_once_with("amq.rabbitmq.reply-to", ensure=False)
        mock_queue.consume.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_consume_no_ack_passed_to_queue_consume(self) -> None:
        """C2: no_ack=True is threaded through to Queue.consume()."""
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        mock_queue = AsyncMock()
        channel.get_queue = AsyncMock(return_value=mock_queue)

        await transport.consume("amq.rabbitmq.reply-to", AsyncMock(), no_ack=True, declare=False)

        mock_queue.consume.assert_awaited_once()
        assert mock_queue.consume.call_args.kwargs["no_ack"] is True

    @pytest.mark.asyncio
    async def test_consume_default_declare_true_no_ack_false(self) -> None:
        """Ordinary consume() keeps declaring the queue and manual ack."""
        transport = _make_transport()
        channel = await self._connect_transport(transport)

        mock_queue = AsyncMock()
        channel.declare_queue = AsyncMock(return_value=mock_queue)
        channel.get_queue = AsyncMock(return_value=mock_queue)

        await transport.consume("orders", AsyncMock())

        channel.declare_queue.assert_awaited_once_with("orders", passive=True)
        channel.get_queue.assert_not_called()
        assert mock_queue.consume.call_args.kwargs["no_ack"] is False


# ── Message Building ────────────────────────────────────────────────────


class TestReconnectCallbacks:
    """Connection-churn metric hook: on_reconnect registers callbacks that
    _aio_reconnected (adapted from aio-pika's reconnect_callbacks) fires."""

    def test_reconnect_callback_fires(self) -> None:
        transport = _make_transport()
        fired: list[int] = []
        transport.on_reconnect(lambda: fired.append(1))

        transport._aio_reconnected()
        transport._aio_reconnected(MagicMock())  # aio-pika passes the sender

        assert fired == [1, 1]

    def test_reconnect_callback_exception_does_not_break_others(self) -> None:
        transport = _make_transport()
        fired: list[int] = []
        transport.on_reconnect(lambda: (_ for _ in ()).throw(RuntimeError("cb boom")))
        transport.on_reconnect(lambda: fired.append(1))

        transport._aio_reconnected()  # must not raise

        assert fired == [1]


class TestBuildMessage:
    def test_build_message_surfaces_timestamp(self) -> None:
        """Regression: msg.timestamp was never populated on consume (always None)."""
        from datetime import UTC, datetime

        transport = _make_transport()
        ts = datetime(2026, 1, 2, 3, 4, 5, tzinfo=UTC)
        mock_aio_msg = MagicMock()
        mock_aio_msg.body = b"{}"
        mock_aio_msg.headers = {}
        mock_aio_msg.timestamp = ts

        message = transport._build_message(mock_aio_msg)

        assert message.timestamp == ts

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

    def test_build_message_surfaces_priority_user_id_and_reencodes_expiration(self) -> None:
        """priority/expiration/user_id used to be dropped entirely on consume
        (RabbitMessage had no slots for them). expiration additionally needs
        re-encoding: aio-pika decodes the wire's ms-string expiration into
        seconds (float) on IncomingMessage, but RabbitMessage/MessageEnvelope
        use the ms-string convention everywhere else (matching pika's raw
        properties.expiration) -- passing the decoded seconds value straight
        through would silently corrupt a retry/DLQ-replay envelope's TTL by
        1000x."""
        transport = _make_transport()
        mock_aio_msg = MagicMock()
        mock_aio_msg.body = b"{}"
        mock_aio_msg.headers = {}
        mock_aio_msg.priority = 8
        mock_aio_msg.expiration = 60.0  # aio-pika: decoded seconds
        mock_aio_msg.user_id = "guest"

        message = transport._build_message(mock_aio_msg)

        assert message.priority == 8
        assert message.expiration == "60000"  # re-encoded to ms-string
        assert message.user_id == "guest"

    def test_build_message_expiration_none_stays_none(self) -> None:
        transport = _make_transport()
        mock_aio_msg = MagicMock()
        mock_aio_msg.body = b"{}"
        mock_aio_msg.headers = {}
        mock_aio_msg.expiration = None

        message = transport._build_message(mock_aio_msg)

        assert message.expiration is None

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

    def test_build_message_no_ack_skips_settlement_wiring(self) -> None:
        """C2: a no-ack delivery (e.g. amq.rabbitmq.reply-to) gets no ack/nack/
        reject functions — the broker already auto-acked it, and aio-pika's
        IncomingMessage.ack()/nack()/reject() raise TypeError on a no-ack
        message anyway."""
        transport = _make_transport()

        mock_aio_msg = MagicMock()
        mock_aio_msg.body = b"reply-body"
        mock_aio_msg.headers = {}
        mock_aio_msg.message_id = None
        mock_aio_msg.correlation_id = "cid-1"
        mock_aio_msg.reply_to = None
        mock_aio_msg.content_type = None
        mock_aio_msg.content_encoding = None
        mock_aio_msg.type = None
        mock_aio_msg.app_id = None
        mock_aio_msg.routing_key = "amq.rabbitmq.reply-to"
        mock_aio_msg.exchange = ""
        mock_aio_msg.delivery_tag = 1
        mock_aio_msg.redelivered = False
        mock_aio_msg.consumer_tag = "rpc-tag"

        message = transport._build_message(mock_aio_msg, no_ack=True)

        assert message._ack_async_fn is None
        assert message._nack_async_fn is None
        assert message._reject_async_fn is None

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


# ── on_blocked / on_unblocked callbacks ──────────────────────────────────


class TestBlockedUnblockedCallbacks:
    def test_on_blocked_registers_callback(self) -> None:
        """Line 94: on_blocked() appends callback to _blocked_callbacks."""
        transport = _make_transport()

        def cb() -> None:
            pass

        transport.on_blocked(cb)
        assert cb in transport._blocked_callbacks

    def test_on_unblocked_registers_callback(self) -> None:
        """Line 98: on_unblocked() appends callback to _unblocked_callbacks."""
        transport = _make_transport()

        def cb() -> None:
            pass

        transport.on_unblocked(cb)
        assert cb in transport._unblocked_callbacks

    def test_aio_blocked_calls_all_callbacks(self) -> None:
        """Lines 101-103: _aio_blocked() calls every registered blocked callback."""
        transport = _make_transport()
        called: list[str] = []
        transport.on_blocked(lambda: called.append("cb1"))
        transport.on_blocked(lambda: called.append("cb2"))
        transport._aio_blocked()
        assert called == ["cb1", "cb2"]

    def test_aio_unblocked_calls_all_callbacks(self) -> None:
        """Lines 108-110: _aio_unblocked() calls every registered unblocked callback."""
        transport = _make_transport()
        called: list[str] = []
        transport.on_unblocked(lambda: called.append("cb1"))
        transport.on_unblocked(lambda: called.append("cb2"))
        transport._aio_unblocked()
        assert called == ["cb1", "cb2"]

    def test_aio_blocked_with_no_callbacks_is_noop(self) -> None:
        """_aio_blocked() with empty list does nothing."""
        transport = _make_transport()
        transport._aio_blocked()  # should not raise

    def test_aio_unblocked_with_no_callbacks_is_noop(self) -> None:
        """_aio_unblocked() with empty list does nothing."""
        transport = _make_transport()
        transport._aio_unblocked()  # should not raise

    def test_is_blocked_tracked_without_any_callback(self) -> None:
        """L15: is_blocked reflects connection.blocked/unblocked frames even
        with zero on_blocked/on_unblocked callbacks registered -- health.py
        reads this directly when no FlowController is wired."""
        transport = _make_transport()
        assert transport.is_blocked is False

        transport._aio_blocked()
        assert transport.is_blocked is True

        transport._aio_unblocked()
        assert transport.is_blocked is False


# ── async context manager ─────────────────────────────────────────────────


class TestAsyncContextManager:
    @pytest.mark.asyncio
    async def test_aenter_connects_and_returns_self(self) -> None:
        """Lines 152-153: __aenter__ connects and returns the transport."""
        transport = _make_transport()
        mock_connection = _make_mock_connection()

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                result = await transport.__aenter__()

        assert result is transport
        assert transport.is_connected()

    @pytest.mark.asyncio
    async def test_async_with_protocol_works_end_to_end(self) -> None:
        """M1 (architect review): the method was named __exit__, so a real
        `async with AsyncTransportImpl(...)` raised TypeError on entry —
        and the old test masked it by calling __exit__ directly. This test
        uses the actual protocol."""
        transport = _make_transport()
        mock_connection = _make_mock_connection()

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                async with transport as entered:
                    assert entered is transport
                    assert transport.is_connected()

        assert not transport.is_connected()


# ── disconnect fast_publish_channel ──────────────────────────────────────


class TestDisconnectFastChannel:
    @pytest.mark.asyncio
    async def test_disconnect_closes_fast_publish_channel(self) -> None:
        """Lines 184-187: disconnect() closes an open fast_publish_channel."""
        transport = _make_transport()
        mock_connection = _make_mock_connection()

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()

        # Inject an open fast publish channel
        fast_ch = MagicMock()
        fast_ch.is_closed = False
        fast_ch.close = AsyncMock()
        transport._fast_publish_channel = fast_ch

        await transport.disconnect()

        fast_ch.close.assert_called_once()
        assert transport._fast_publish_channel is None

    @pytest.mark.asyncio
    async def test_disconnect_skips_closed_fast_publish_channel(self) -> None:
        """disconnect() does NOT call close() on an already-closed fast channel."""
        transport = _make_transport()
        mock_connection = _make_mock_connection()

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()

        fast_ch = MagicMock()
        fast_ch.is_closed = True
        fast_ch.close = AsyncMock()
        transport._fast_publish_channel = fast_ch

        await transport.disconnect()

        fast_ch.close.assert_not_called()


# ── is_connected edge cases ───────────────────────────────────────────────


class TestIsConnectedEdgeCases:
    def test_is_connected_returns_false_when_publisher_connection_is_none(self) -> None:
        """Line 210: is_connected() returns False when _publisher_connection is None."""
        transport = _make_transport()
        transport._connected = True
        transport._conn_pool._publisher_connection = None
        assert transport.is_connected() is False

    def test_is_connected_returns_false_when_connection_is_closed(self) -> None:
        """Line 214: is_connected() returns False when connection.is_closed is True."""
        transport = _make_transport()
        transport._connected = True
        mock_conn = MagicMock()
        mock_conn.is_closed = True
        transport._conn_pool._publisher_connection = mock_conn
        assert transport.is_connected() is False

    def test_is_connected_returns_true_when_open(self) -> None:
        """is_connected() returns True when connected and is_closed is False."""
        transport = _make_transport()
        transport._connected = True
        mock_conn = MagicMock()
        mock_conn.is_closed = False
        transport._conn_pool._publisher_connection = mock_conn
        assert transport.is_connected() is True


# ── has_open_channels ────────────────────────────────────────────────────


class TestHasOpenChannels:
    def test_has_open_channels_false_when_no_consumer_channels(self) -> None:
        """Line 226-227: returns False when _consumer_channels is empty."""
        transport = _make_transport()
        assert transport._consumer_channels == {}
        assert transport.has_open_channels is False

    def test_has_open_channels_true_when_all_open(self) -> None:
        """Line 228: returns True when all channels are open."""
        transport = _make_transport()
        ch = MagicMock()
        ch.is_closed = False
        transport._consumer_channels["q1"] = ch
        assert transport.has_open_channels is True

    def test_has_open_channels_false_when_any_closed(self) -> None:
        """Line 228: returns False when at least one channel is closed."""
        transport = _make_transport()
        open_ch = MagicMock()
        open_ch.is_closed = False
        closed_ch = MagicMock()
        closed_ch.is_closed = True
        transport._consumer_channels["q1"] = open_ch
        transport._consumer_channels["q2"] = closed_ch
        assert transport.has_open_channels is False


# ── is_reconnecting ──────────────────────────────────────────────────────


class TestIsReconnecting:
    def test_is_reconnecting_false_when_no_publisher_connection(self) -> None:
        """Lines 237-238: returns False when _publisher_connection is None."""
        transport = _make_transport()
        transport._conn_pool._publisher_connection = None
        assert transport.is_reconnecting is False

    def test_is_reconnecting_false_when_no_reconnect_lock(self) -> None:
        """Lines 242-244: returns False when connection has no _reconnect_lock."""
        transport = _make_transport()
        mock_conn = MagicMock(spec=[])  # no _reconnect_lock attribute
        transport._conn_pool._publisher_connection = mock_conn
        assert transport.is_reconnecting is False

    def test_is_reconnecting_true_when_lock_is_locked(self) -> None:
        """Lines 242-244: returns True when _reconnect_lock.locked() is True."""
        transport = _make_transport()
        mock_lock = MagicMock()
        mock_lock.locked.return_value = True
        mock_conn = MagicMock()
        mock_conn._reconnect_lock = mock_lock
        transport._conn_pool._publisher_connection = mock_conn
        assert transport.is_reconnecting is True

    def test_is_reconnecting_false_when_lock_not_locked(self) -> None:
        """Returns False when _reconnect_lock.locked() is False."""
        transport = _make_transport()
        mock_lock = MagicMock()
        mock_lock.locked.return_value = False
        mock_conn = MagicMock()
        mock_conn._reconnect_lock = mock_lock
        transport._conn_pool._publisher_connection = mock_conn
        assert transport.is_reconnecting is False


# ── _get_fast_channel ────────────────────────────────────────────────────


class TestGetFastChannel:
    @pytest.mark.asyncio
    async def test_get_fast_channel_creates_new_channel(self) -> None:
        """Lines 262-273: _get_fast_channel() creates a channel when none exists."""
        transport = _make_transport()
        mock_connection = _make_mock_connection()

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()

        # Inject publisher connection
        mock_pub_conn = MagicMock()
        new_channel = AsyncMock()
        mock_pub_conn.channel = AsyncMock(return_value=new_channel)
        transport._conn_pool._publisher_connection = mock_pub_conn

        ch = await transport._get_fast_channel()
        assert ch is new_channel
        mock_pub_conn.channel.assert_called_once_with(publisher_confirms=False)

    @pytest.mark.asyncio
    async def test_get_fast_channel_reuses_existing_open_channel(self) -> None:
        """Lines 262-264: _get_fast_channel() returns existing open channel."""
        transport = _make_transport()
        existing_ch = MagicMock()
        existing_ch.is_closed = False
        transport._fast_publish_channel = existing_ch

        ch = await transport._get_fast_channel()
        assert ch is existing_ch

    @pytest.mark.asyncio
    async def test_get_fast_channel_reopens_closed_channel(self) -> None:
        """Lines 265-273: _get_fast_channel() reopens a closed fast channel."""
        transport = _make_transport()
        closed_ch = MagicMock()
        closed_ch.is_closed = True
        transport._fast_publish_channel = closed_ch

        mock_pub_conn = MagicMock()
        new_ch = AsyncMock()
        mock_pub_conn.channel = AsyncMock(return_value=new_ch)
        transport._conn_pool._publisher_connection = mock_pub_conn

        ch = await transport._get_fast_channel()
        assert ch is new_ch

    @pytest.mark.asyncio
    async def test_get_fast_channel_raises_when_no_publisher_connection(self) -> None:
        """Line 270-271: _get_fast_channel() raises RuntimeError when no conn."""
        transport = _make_transport()
        transport._conn_pool._publisher_connection = None

        with pytest.raises(RuntimeError, match="Publisher connection is not available"):
            await transport._get_fast_channel()


# ── _get_mandatory_channel (H1) ──────────────────────────────────────────


class TestGetMandatoryChannel:
    @pytest.mark.asyncio
    async def test_get_mandatory_channel_reuses_existing_open_channel(self) -> None:
        """Line 317: _get_mandatory_channel() returns existing open channel."""
        transport = _make_transport()
        existing_ch = MagicMock()
        existing_ch.is_closed = False
        transport._mandatory_publish_channel = existing_ch

        ch = await transport._get_mandatory_channel()
        assert ch is existing_ch

    @pytest.mark.asyncio
    async def test_get_mandatory_channel_raises_when_no_publisher_connection(self) -> None:
        """Line 324: _get_mandatory_channel() raises RuntimeError when no conn."""
        transport = _make_transport()
        transport._conn_pool._publisher_connection = None

        with pytest.raises(RuntimeError, match="Publisher connection is not available"):
            await transport._get_mandatory_channel()


# ── _publish_on_channel timeout ───────────────────────────────────────────


class TestPublishOnChannelTimeout:
    @pytest.mark.asyncio
    async def test_publish_on_channel_returns_timeout_outcome(self) -> None:
        """Lines 317-328: _publish_on_channel() returns TIMEOUT outcome on TimeoutError."""
        transport = _make_transport(confirm_timeout=0.01)
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_exchange.publish.side_effect = TimeoutError("timeout")
        mock_channel.get_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.default_exchange = mock_exchange

        envelope = MessageEnvelope(routing_key="rk", body=b"x", exchange="ex")

        with patch("aio_pika.Message") as mock_msg_cls:
            mock_msg_cls.return_value = MagicMock()
            with patch("aio_pika.DeliveryMode") as mock_dm:
                mock_dm.return_value = 2
                # Patch asyncio.timeout to just raise TimeoutError immediately
                with patch("asyncio.timeout") as mock_timeout:
                    class _FakeCtx:
                        async def __aenter__(self):
                            return self
                        async def __aexit__(self, exc_type, exc, tb):
                            return False

                    mock_timeout.return_value = _FakeCtx()
                    mock_exchange.publish.side_effect = TimeoutError("broker stalled")

                    outcome = await transport._publish_on_channel(mock_channel, envelope)

        assert outcome.status == PublishStatus.TIMEOUT
        assert outcome.error is not None

    @pytest.mark.asyncio
    async def test_publish_on_channel_does_not_close_channel_itself(self) -> None:
        """M17: closing on timeout is the CALLER's responsibility now (see
        publish()'s mandatory/pool branches and AsyncBatchPublisher._flush) —
        _publish_on_channel itself must never close a channel it doesn't own
        exclusively."""
        transport = _make_transport(confirm_timeout=0.01)
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_exchange = AsyncMock()
        mock_exchange.publish.side_effect = TimeoutError("timeout")
        mock_channel.get_exchange = AsyncMock(return_value=mock_exchange)
        mock_channel.close = AsyncMock()

        envelope = MessageEnvelope(routing_key="rk", body=b"x", exchange="ex")

        with patch("aio_pika.Message"), patch("aio_pika.DeliveryMode"):
            outcome = await transport._publish_on_channel(mock_channel, envelope)

        assert outcome.status == PublishStatus.TIMEOUT
        mock_channel.close.assert_not_called()


class TestMandatoryChannelClosedOnTimeout:
    """M17: publish()'s mandatory=True branch closes the (shared, persistent)
    mandatory channel AFTER its own call resolves -- not from inside
    _publish_on_channel, which cannot know if it's the sole user."""

    @pytest.mark.asyncio
    async def test_timeout_closes_mandatory_channel_for_next_publish(self) -> None:
        transport = _make_transport(confirm_timeout=0.01)
        transport._connected = True  # keep publish() off the real connect() path
        mock_conn = _make_mock_connection()
        transport._conn_pool._publisher_connection = mock_conn

        timed_out_channel = AsyncMock()
        timed_out_channel.is_closed = False

        async def _close() -> None:
            timed_out_channel.is_closed = True  # real aio-pika flips this on close()

        timed_out_channel.close = AsyncMock(side_effect=_close)
        fresh_channel = AsyncMock()
        fresh_channel.is_closed = False

        mock_conn.channel = AsyncMock(side_effect=[timed_out_channel, fresh_channel])

        with patch.object(
            transport, "_publish_on_channel", new_callable=AsyncMock
        ) as mock_pub:
            mock_pub.return_value = PublishOutcome(status=PublishStatus.TIMEOUT, exchange="", routing_key="q")
            envelope = MessageEnvelope(routing_key="q", body=b"x", mandatory=True)
            outcome = await transport.publish(envelope)

        assert outcome.status == PublishStatus.TIMEOUT
        timed_out_channel.close.assert_called_once()
        # Next mandatory publish must get a FRESH channel (the old one was
        # closed, so _get_mandatory_channel()'s is_closed check reopens it).
        next_ch = await transport._get_mandatory_channel()
        assert next_ch is fresh_channel

    @pytest.mark.asyncio
    async def test_confirmed_mandatory_publish_does_not_close_channel(self) -> None:
        transport = _make_transport(confirm_timeout=0.01)
        transport._connected = True  # keep publish() off the real connect() path
        mock_conn = _make_mock_connection()
        transport._conn_pool._publisher_connection = mock_conn
        channel = AsyncMock()
        channel.is_closed = False
        channel.close = AsyncMock()
        mock_conn.channel = AsyncMock(return_value=channel)

        with patch.object(transport, "_publish_on_channel", new_callable=AsyncMock) as mock_pub:
            mock_pub.return_value = PublishOutcome(status=PublishStatus.CONFIRMED, exchange="", routing_key="q")
            envelope = MessageEnvelope(routing_key="q", body=b"x", mandatory=True)
            await transport.publish(envelope)

        channel.close.assert_not_called()


class TestPooledChannelClosedOnTimeout:
    """M17: the confirmed-pool publish path closes a timed-out channel before
    releasing it back to the pool, so the pool doesn't hand out a wedged
    channel on the next acquire."""

    @pytest.mark.asyncio
    async def test_timeout_closes_channel_before_release(self) -> None:
        transport = _make_transport(confirm_delivery=True, confirm_timeout=0.01)
        channel = AsyncMock()
        channel.is_closed = False

        async def _close() -> None:
            channel.is_closed = True  # real aio-pika flips this on close()

        channel.close = AsyncMock(side_effect=_close)
        transport._conn_pool.acquire_publisher_channel = AsyncMock(return_value=channel)
        release_calls: list[Any] = []

        async def release(channel: Any) -> None:
            release_calls.append(channel.is_closed)

        transport._conn_pool.release_publisher_channel = release
        transport._connected = True

        with patch.object(transport, "_publish_on_channel", new_callable=AsyncMock) as mock_pub:
            mock_pub.return_value = PublishOutcome(status=PublishStatus.TIMEOUT, exchange="", routing_key="q")
            outcome = await transport.publish(MessageEnvelope(routing_key="q", body=b"x"))

        assert outcome.status == PublishStatus.TIMEOUT
        channel.close.assert_called_once()
        # Released AFTER being closed, so the pool sees is_closed=True.
        assert release_calls == [True]

    @pytest.mark.asyncio
    async def test_confirmed_publish_does_not_close_pooled_channel(self) -> None:
        transport = _make_transport(confirm_delivery=True, confirm_timeout=0.01)
        channel = AsyncMock()
        channel.is_closed = False
        channel.close = AsyncMock()
        transport._conn_pool.acquire_publisher_channel = AsyncMock(return_value=channel)
        transport._conn_pool.release_publisher_channel = AsyncMock()
        transport._connected = True

        with patch.object(transport, "_publish_on_channel", new_callable=AsyncMock) as mock_pub:
            mock_pub.return_value = PublishOutcome(status=PublishStatus.CONFIRMED, exchange="", routing_key="q")
            await transport.publish(MessageEnvelope(routing_key="q", body=b"x"))

        channel.close.assert_not_called()


# ── publish fast-path (no confirm) ───────────────────────────────────────


class TestPublishFastPath:
    async def _connected_transport(self, confirm_delivery: bool = False) -> AsyncTransportImpl:
        transport = _make_transport(confirm_delivery=confirm_delivery)
        mock_connection = _make_mock_connection()
        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()
        return transport

    @pytest.mark.asyncio
    async def test_publish_fast_path_when_not_connected_triggers_ensure_connected(self) -> None:
        """Line 344-345: publish() calls _ensure_connected() when not yet connected."""
        transport = _make_transport(confirm_delivery=False)
        mock_connection = _make_mock_connection()

        ensure_called: list[bool] = []
        original_ensure = transport._ensure_connected

        async def spy_ensure() -> None:
            ensure_called.append(True)
            await original_ensure()

        transport._ensure_connected = spy_ensure  # type: ignore[method-assign]

        mock_exchange = AsyncMock()
        mock_connection.channel.return_value.get_exchange = AsyncMock(return_value=mock_exchange)

        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                with patch("aio_pika.Message") as mock_msg_cls:
                    with patch("aio_pika.DeliveryMode") as mock_dm:
                        mock_dm.return_value = 2
                        mock_msg_cls.return_value = MagicMock()
                        outcome = await transport.publish(
                            MessageEnvelope(routing_key="rk", body=b"x", exchange="ex")
                        )

        assert len(ensure_called) == 1
        assert outcome.status == PublishStatus.SENT  # M4: confirm_delivery=False -> SENT, not CONFIRMED

    @pytest.mark.asyncio
    async def test_publish_no_confirm_fast_path(self) -> None:
        """Lines 349-365: publish() with confirm_delivery=False uses fast channel."""
        transport = await self._connected_transport(confirm_delivery=False)

        mock_pub_conn = MagicMock()
        mock_fast_ch = AsyncMock()
        mock_fast_ch.is_closed = False
        mock_exchange = AsyncMock()
        mock_fast_ch.get_exchange = AsyncMock(return_value=mock_exchange)
        mock_fast_ch.default_exchange = mock_exchange
        mock_pub_conn.channel = AsyncMock(return_value=mock_fast_ch)
        transport._conn_pool._publisher_connection = mock_pub_conn

        # Inject the fast channel directly so _get_fast_channel returns it
        transport._fast_publish_channel = mock_fast_ch

        envelope = MessageEnvelope(routing_key="rk", body=b"x", exchange="ex")
        with patch("aio_pika.Message") as mock_msg_cls:
            with patch("aio_pika.DeliveryMode") as mock_dm:
                mock_dm.return_value = 2
                mock_msg_cls.return_value = MagicMock()
                outcome = await transport.publish(envelope)

        assert outcome.status == PublishStatus.SENT  # M4: confirm_delivery=False -> SENT, not CONFIRMED
        mock_exchange.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_no_confirm_fast_path_default_exchange(self) -> None:
        """Fast path uses default_exchange when exchange name is empty."""
        transport = await self._connected_transport(confirm_delivery=False)

        mock_fast_ch = AsyncMock()
        mock_fast_ch.is_closed = False
        mock_exchange = AsyncMock()
        mock_fast_ch.default_exchange = mock_exchange
        transport._fast_publish_channel = mock_fast_ch

        envelope = MessageEnvelope(routing_key="rk", body=b"x", exchange="")
        with patch("aio_pika.Message") as mock_msg_cls:
            with patch("aio_pika.DeliveryMode") as mock_dm:
                mock_dm.return_value = 2
                mock_msg_cls.return_value = MagicMock()
                outcome = await transport.publish(envelope)

        assert outcome.status == PublishStatus.SENT  # M4: confirm_delivery=False -> SENT, not CONFIRMED
        mock_fast_ch.get_exchange.assert_not_called()
        mock_exchange.publish.assert_called_once()


# ── C2: direct reply-to channel affinity ──────────────────────────────────


class TestReplyToChannelAffinity:
    """C2: RabbitMQ's direct reply-to requires the reply consumer and the
    corresponding request publish to happen on the SAME channel."""

    async def _connected_transport(self, confirm_delivery: bool = False) -> AsyncTransportImpl:
        transport = _make_transport(confirm_delivery=confirm_delivery)
        mock_connection = _make_mock_connection()
        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()
        return transport

    @pytest.mark.asyncio
    async def test_consume_declare_false_reply_to_queue_tracks_channel(self) -> None:
        transport = await self._connected_transport()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_queue = AsyncMock()

        with patch.object(transport, "_conn_pool") as mock_pool:
            mock_conn = AsyncMock()
            mock_conn.channel = AsyncMock(return_value=mock_channel)
            mock_pool.get_consumer_connection = AsyncMock(return_value=mock_conn)
            mock_channel.get_queue = AsyncMock(return_value=mock_queue)

            await transport.consume("amq.rabbitmq.reply-to", AsyncMock(), no_ack=True, declare=False)

        assert transport._reply_to_channel is mock_channel

    @pytest.mark.asyncio
    async def test_consume_declare_true_does_not_track_reply_to_channel(self) -> None:
        transport = await self._connected_transport()
        mock_channel = AsyncMock()
        mock_channel.is_closed = False
        mock_queue = AsyncMock()

        with patch.object(transport, "_conn_pool") as mock_pool:
            mock_conn = AsyncMock()
            mock_conn.channel = AsyncMock(return_value=mock_channel)
            mock_pool.get_consumer_connection = AsyncMock(return_value=mock_conn)
            mock_channel.declare_queue = AsyncMock(return_value=mock_queue)

            await transport.consume("orders", AsyncMock())

        assert transport._reply_to_channel is None

    @pytest.mark.asyncio
    async def test_publish_reply_to_routes_onto_reply_channel(self) -> None:
        """A request with reply_to=amq.rabbitmq.reply-to must publish via
        _reply_to_channel, bypassing the normal fast/pool paths entirely."""
        transport = await self._connected_transport(confirm_delivery=False)

        reply_channel = AsyncMock()
        reply_channel.is_closed = False
        transport._reply_to_channel = reply_channel

        captured: list[Any] = []

        async def fake_publish_on_channel(channel: Any, envelope: MessageEnvelope) -> PublishOutcome:
            captured.append(channel)
            return PublishOutcome(
                status=PublishStatus.CONFIRMED, exchange=envelope.exchange, routing_key=envelope.routing_key
            )

        transport._publish_on_channel = fake_publish_on_channel  # type: ignore[method-assign]

        envelope = MessageEnvelope(routing_key="rpc.q", body=b"req", reply_to="amq.rabbitmq.reply-to")
        outcome = await transport.publish(envelope)

        assert outcome.ok
        assert captured == [reply_channel]

    @pytest.mark.asyncio
    async def test_publish_without_reply_to_ignores_reply_channel(self) -> None:
        """A normal publish must not touch _reply_to_channel even if one is set."""
        transport = await self._connected_transport(confirm_delivery=False)

        reply_channel = AsyncMock()
        reply_channel.is_closed = False
        transport._reply_to_channel = reply_channel

        mock_fast_ch = AsyncMock()
        mock_fast_ch.is_closed = False
        mock_exchange = AsyncMock()
        mock_fast_ch.default_exchange = mock_exchange
        transport._fast_publish_channel = mock_fast_ch

        envelope = MessageEnvelope(routing_key="orders", body=b"data", exchange="")
        with patch("aio_pika.Message") as mock_msg_cls:
            with patch("aio_pika.DeliveryMode") as mock_dm:
                mock_dm.return_value = 2
                mock_msg_cls.return_value = MagicMock()
                outcome = await transport.publish(envelope)

        assert outcome.status == PublishStatus.SENT  # M4: confirm_delivery=False -> SENT, not CONFIRMED
        reply_channel.basic_publish.assert_not_called()
        mock_exchange.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_reply_to_falls_back_when_reply_channel_closed(self) -> None:
        """A closed _reply_to_channel must not be used — fall through to the
        normal publish path instead of erroring on a dead channel."""
        transport = await self._connected_transport(confirm_delivery=False)

        reply_channel = AsyncMock()
        reply_channel.is_closed = True  # stale/closed
        transport._reply_to_channel = reply_channel

        mock_fast_ch = AsyncMock()
        mock_fast_ch.is_closed = False
        mock_exchange = AsyncMock()
        mock_fast_ch.default_exchange = mock_exchange
        transport._fast_publish_channel = mock_fast_ch

        envelope = MessageEnvelope(routing_key="rpc.q", body=b"req", reply_to="amq.rabbitmq.reply-to", exchange="")
        with patch("aio_pika.Message") as mock_msg_cls:
            with patch("aio_pika.DeliveryMode") as mock_dm:
                mock_dm.return_value = 2
                mock_msg_cls.return_value = MagicMock()
                outcome = await transport.publish(envelope)

        assert outcome.status == PublishStatus.SENT  # M4: confirm_delivery=False -> SENT, not CONFIRMED
        reply_channel.basic_publish.assert_not_called()
        mock_exchange.publish.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancel_consumer_clears_reply_to_channel(self) -> None:
        transport = await self._connected_transport()
        reply_channel = AsyncMock()
        reply_channel.is_closed = False
        transport._reply_to_channel = reply_channel
        transport._consumer_channels["amq.rabbitmq.reply-to"] = reply_channel
        transport._consumer_tags["amq.rabbitmq.reply-to"] = "reply-tag"

        mock_queue = AsyncMock()
        reply_channel.get_queue = AsyncMock(return_value=mock_queue)

        await transport.cancel_consumer("reply-tag")

        assert transport._reply_to_channel is None


# ── basic_get / purge_queue ───────────────────────────────────────────────


class TestBasicGetAndPurge:
    async def _connected_transport(self) -> tuple[AsyncTransportImpl, AsyncMock]:
        transport = _make_transport()
        mock_connection = _make_mock_connection()
        with patch("rabbitkit.async_.pool.make_aio_pika_connect_kwargs", return_value={"url": "amqp://guest:guest@localhost/"}):
            with patch("aio_pika.connect_robust", new_callable=AsyncMock, return_value=mock_connection):
                await transport.connect()
        topo_ch = mock_connection.channel.return_value
        transport._topology_channel = topo_ch
        return transport, topo_ch

    @pytest.mark.asyncio
    async def test_basic_get_returns_none_when_queue_empty(self) -> None:
        """Lines 518-523: basic_get() returns None when queue is empty."""
        transport, topo_ch = await self._connected_transport()

        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(return_value=None)
        topo_ch.get_queue = AsyncMock(return_value=mock_queue)

        result = await transport.basic_get("orders.dlq")
        assert result is None

    @pytest.mark.asyncio
    async def test_basic_get_returns_message_when_present(self) -> None:
        """Line 524: basic_get() builds and returns a RabbitMessage."""
        transport, topo_ch = await self._connected_transport()

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
        fake_aio_msg.routing_key = "rk"
        fake_aio_msg.exchange = ""
        fake_aio_msg.delivery_tag = 1
        fake_aio_msg.redelivered = False
        fake_aio_msg.consumer_tag = "t"

        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(return_value=fake_aio_msg)
        topo_ch.get_queue = AsyncMock(return_value=mock_queue)

        result = await transport.basic_get("orders.dlq")
        assert result is not None
        assert result.body == b"payload"

    @pytest.mark.asyncio
    async def test_purge_queue_returns_message_count(self) -> None:
        """Lines 528-532: purge_queue() returns int message count."""
        transport, topo_ch = await self._connected_transport()

        purge_result = MagicMock()
        purge_result.message_count = 42

        mock_queue = AsyncMock()
        mock_queue.purge = AsyncMock(return_value=purge_result)
        topo_ch.get_queue = AsyncMock(return_value=mock_queue)

        count = await transport.purge_queue("orders.dlq")
        assert count == 42

    @pytest.mark.asyncio
    async def test_purge_queue_returns_zero_when_no_message_count(self) -> None:
        """purge_queue() returns 0 when result has no message_count attribute."""
        transport, topo_ch = await self._connected_transport()

        mock_queue = AsyncMock()
        mock_queue.purge = AsyncMock(return_value=MagicMock(spec=[]))  # no message_count
        topo_ch.get_queue = AsyncMock(return_value=mock_queue)

        count = await transport.purge_queue("orders.dlq")
        assert count == 0


# ── Architect review M3: shared mandatory channel must survive one caller's
# confirm timeout while siblings are in flight ────────────────────────────


class TestMandatoryChannelTimeoutRecycle:
    @pytest.mark.asyncio
    async def test_timeout_does_not_close_channel_while_sibling_in_flight(self) -> None:
        """One mandatory publish timing out while another is still awaiting
        its own confirm must NOT close the shared channel under the sibling —
        it is recycled only when the LAST in-flight publish resolves."""
        import asyncio as _asyncio

        from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus

        transport = _make_transport()
        transport._connected = True

        channel = AsyncMock()
        channel.is_closed = False
        transport._mandatory_publish_channel = channel

        async def get_channel() -> AsyncMock:
            return channel

        transport._get_mandatory_channel = get_channel  # type: ignore[method-assign]

        slow_gate = _asyncio.Event()
        outcomes: dict[str, PublishStatus] = {}

        async def fake_publish_on_channel(ch: AsyncMock, envelope: MessageEnvelope) -> PublishOutcome:
            if envelope.routing_key == "slow":
                await slow_gate.wait()
                return PublishOutcome(status=PublishStatus.CONFIRMED, routing_key="slow")
            return PublishOutcome(status=PublishStatus.TIMEOUT, routing_key="fast")

        transport._publish_on_channel = fake_publish_on_channel  # type: ignore[method-assign]

        slow_task = _asyncio.create_task(
            transport.publish(MessageEnvelope(routing_key="slow", body=b"x", mandatory=True))
        )
        await _asyncio.sleep(0)  # let the slow publish enter the in-flight section

        fast = await transport.publish(MessageEnvelope(routing_key="fast", body=b"x", mandatory=True))
        outcomes["fast"] = fast.status

        # The fast publish TIMED OUT — but the slow sibling is still in
        # flight, so the shared channel must NOT have been closed yet.
        channel.close.assert_not_called()
        assert transport._mandatory_channel_recycle is True

        slow_gate.set()
        slow = await slow_task
        outcomes["slow"] = slow.status

        # Last in-flight resolved → deferred recycle now closes the channel.
        channel.close.assert_called_once()
        assert transport._mandatory_channel_recycle is False
        assert outcomes == {"fast": PublishStatus.TIMEOUT, "slow": PublishStatus.CONFIRMED}


class TestBindingRestoreAfterReconnect:
    """Verification gap 3/4: bindings recorded by bind_queue/bind_exchange
    are re-applied after a robust reconnect (they are not in RobustChannel's
    restoration registry)."""

    @pytest.mark.asyncio
    async def test_bind_queue_records_binding(self) -> None:
        transport = _make_transport()
        transport._connected = True
        topo = AsyncMock()
        topo.is_closed = False
        transport._topology_channel = topo

        await transport.bind_queue(queue="orders", exchange="events", routing_key="orders.*")

        assert ("queue", "orders", "events", "orders.*", None) in transport._recorded_bindings

    @pytest.mark.asyncio
    async def test_reapply_bindings_rebinds_and_stops_when_done(self) -> None:
        import asyncio as _asyncio
        from unittest.mock import patch as _patch

        transport = _make_transport()
        topo = AsyncMock()
        topo.is_closed = False
        transport._topology_channel = topo
        transport._recorded_bindings.append(("queue", "orders", "events", "orders.*", None))

        q = AsyncMock()
        ex = AsyncMock()
        topo.get_queue.return_value = q
        topo.get_exchange.return_value = ex

        with _patch.object(_asyncio, "sleep", new=AsyncMock()):
            await transport._reapply_bindings()

        q.bind.assert_awaited_once_with(ex, routing_key="orders.*", arguments=None)

    @pytest.mark.asyncio
    async def test_reapply_bindings_noop_when_topology_channel_gone(self) -> None:
        """Shutdown-during-reconnect: the task must exit cleanly."""
        import asyncio as _asyncio
        from unittest.mock import patch as _patch

        transport = _make_transport()
        transport._topology_channel = None
        transport._recorded_bindings.append(("queue", "orders", "events", "", None))

        with _patch.object(_asyncio, "sleep", new=AsyncMock()):
            await transport._reapply_bindings()  # must not raise
