"""Tests for sync/transport.py — SyncTransport (mocked pika)."""

from __future__ import annotations

import queue
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

    def test_first_connect_does_not_fire_reconnect_callbacks(self) -> None:
        """Connection-churn counter: the FIRST connect is not churn."""
        transport = _make_transport()
        fired: list[int] = []
        transport.on_reconnect(lambda: fired.append(1))

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection"):
                transport.connect()

        assert fired == []

    def test_reconnect_fires_reconnect_callbacks(self) -> None:
        """Every connect AFTER the first fires the churn hook -- reconnects
        were previously logged but never counted, so a flapping broker/
        network was invisible to metrics-based alerting."""
        transport = _make_transport()
        fired: list[int] = []
        transport.on_reconnect(lambda: fired.append(1))

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True

                transport.connect()
                transport.disconnect()
                transport.connect()  # re-connect -> churn
                assert fired == [1]
                transport.disconnect()
                transport.connect()
                assert fired == [1, 1]

    def test_reconnect_callback_exception_does_not_break_connect(self) -> None:
        transport = _make_transport()
        transport.on_reconnect(lambda: (_ for _ in ()).throw(RuntimeError("cb boom")))

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True

                transport.connect()
                transport.disconnect()
                transport.connect()  # must not raise despite the bad callback

        assert transport.is_connected()

    def test_connect_fires_channel_opened_but_not_rebuilt_first_time(self) -> None:
        """Item 3: the publisher channel opened by the FIRST connect() is a
        fresh open, not a rebuild (nothing existed before to replace)."""
        transport = _make_transport()
        opened: list[int] = []
        rebuilt: list[int] = []
        transport.on_channel_opened(lambda: opened.append(1))
        transport.on_channel_rebuilt(lambda: rebuilt.append(1))

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection"):
                transport.connect()

        assert opened == [1]
        assert rebuilt == []

    def test_reconnect_fires_channel_opened_and_rebuilt(self) -> None:
        """Item 3: a connect() after disconnect() replaces the publisher
        channel -- both channels_opened_total and channel_rebuilds_total
        must increment."""
        transport = _make_transport()
        opened: list[int] = []
        rebuilt: list[int] = []
        transport.on_channel_opened(lambda: opened.append(1))
        transport.on_channel_rebuilt(lambda: rebuilt.append(1))

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True

                transport.connect()
                transport.disconnect()
                transport.connect()

        assert opened == [1, 1]
        assert rebuilt == [1]

    def test_channel_callback_exception_does_not_break_connect(self) -> None:
        transport = _make_transport()
        transport.on_channel_opened(lambda: (_ for _ in ()).throw(RuntimeError("cb boom")))

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_channel = MagicMock()
                mock_channel.is_open = True
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True

                transport.connect()  # must not raise despite the bad callback

        assert transport.is_connected()

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

    def test_declare_queue_precondition_failed_raises_configuration_error(self) -> None:
        """M6: a 406 PRECONDITION_FAILED (e.g. an ops-created queue with
        different arguments) must raise a typed ConfigurationError naming
        the conflicting queue -- not an opaque pika channel-closed error."""
        import pika

        from rabbitkit.core.errors import ConfigurationError

        transport = _make_transport()
        channel = self._connect_transport(transport)
        channel.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(
            406, "PRECONDITION_FAILED - inequivalent arg 'x-queue-type' for queue 'orders'"
        )

        queue = RabbitQueue(name="orders")
        with pytest.raises(ConfigurationError, match="orders") as exc_info:
            transport.declare_queue(queue)

        assert "PRECONDITION_FAILED" in str(exc_info.value)
        assert isinstance(exc_info.value.__cause__, pika.exceptions.ChannelClosedByBroker)

    def test_declare_queue_406_warn_continue(self) -> None:
        """M14: with on_topology_conflict='warn_continue', a 406 is warned and
        swallowed (existing definition kept), the channel is reopened, and
        subsequent declares proceed."""
        import pika

        transport = _make_transport(on_topology_conflict="warn_continue")
        channel = self._connect_transport(transport)
        channel.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(
            406, "PRECONDITION_FAILED - inequivalent arg 'x-queue-type' for queue 'orders'"
        )
        # The reopened channel comes from connection.channel(); make it fresh.
        reopened = MagicMock()
        reopened.is_open = True
        transport._connection.channel.return_value = reopened

        transport.declare_queue(RabbitQueue(name="orders"))  # must NOT raise

        assert transport._channel is reopened  # channel reopened for further declares

    def test_declare_queue_406_raise_is_default(self) -> None:
        """M14: default policy still raises (no silent drift)."""
        import pika

        from rabbitkit.core.errors import ConfigurationError

        transport = _make_transport()  # default on_topology_conflict="raise"
        channel = self._connect_transport(transport)
        channel.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(
            406, "PRECONDITION_FAILED - inequivalent arg"
        )
        with pytest.raises(ConfigurationError):
            transport.declare_queue(RabbitQueue(name="orders"))

    def test_declare_queue_other_channel_closed_reraises(self) -> None:
        """M6: a non-406 channel closure is not this middleware's concern --
        must propagate as-is, not be swallowed or misreported."""
        import pika

        transport = _make_transport()
        channel = self._connect_transport(transport)
        channel.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(
            403, "ACCESS_REFUSED - queue name not allowed"
        )

        queue = RabbitQueue(name="orders")
        with pytest.raises(pika.exceptions.ChannelClosedByBroker):
            transport.declare_queue(queue)

    def test_declare_exchange_precondition_failed_raises_configuration_error(self) -> None:
        """M6: same as the queue case, for exchange declaration."""
        import pika

        from rabbitkit.core.errors import ConfigurationError

        transport = _make_transport()
        channel = self._connect_transport(transport)
        channel.exchange_declare.side_effect = pika.exceptions.ChannelClosedByBroker(
            406, "PRECONDITION_FAILED - inequivalent arg 'type' for exchange 'events'"
        )

        exchange = RabbitExchange(name="events")
        with pytest.raises(ConfigurationError, match="events"):
            transport.declare_exchange(exchange)

    def test_bind_queue(self) -> None:
        transport = _make_transport()
        channel = self._connect_transport(transport)

        transport.bind_queue("orders", "events", "orders.created")

        channel.queue_bind.assert_called_once_with(
            queue="orders", exchange="events", routing_key="orders.created", arguments=None
        )

    def test_bind_queue_passes_arguments(self) -> None:
        """C4: headers-exchange match criteria must reach the broker."""
        transport = _make_transport()
        channel = self._connect_transport(transport)

        args = {"x-match": "all", "type": "order"}
        transport.bind_queue("orders.headers", "events.headers", "", arguments=args)

        channel.queue_bind.assert_called_once_with(
            queue="orders.headers", exchange="events.headers", routing_key="", arguments=args
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

    def test_publish_mandatory_enables_confirms_on_demand(self) -> None:
        """H1: a mandatory=True publish enables confirm_delivery() on the
        target channel even when the transport was constructed with
        confirm_delivery=False — required to reliably detect a return."""
        transport = _make_transport(confirm_delivery=False)
        channel = self._connect_transport(transport)

        envelope = MessageEnvelope(routing_key="rk", body=b"hello", mandatory=True)
        outcome = transport.publish(envelope)

        assert outcome.ok
        channel.confirm_delivery.assert_called_once()

    def test_publish_mandatory_confirms_enabled_only_once(self) -> None:
        """confirm_delivery() must not be called again on a channel that
        already has it enabled — pika logs a spurious error on a repeat call."""
        transport = _make_transport(confirm_delivery=False)
        channel = self._connect_transport(transport)

        envelope = MessageEnvelope(routing_key="rk", body=b"hello", mandatory=True)
        transport.publish(envelope)
        transport.publish(envelope)

        channel.confirm_delivery.assert_called_once()

    def test_publish_mandatory_does_not_reconfirm_already_confirmed_channel(self) -> None:
        """confirm_delivery=True at construction already enabled confirms in
        connect() — a later mandatory=True publish must not call it again."""
        transport = _make_transport(confirm_delivery=True)
        channel = self._connect_transport(transport)
        channel.confirm_delivery.reset_mock()  # connect() already called it once

        envelope = MessageEnvelope(routing_key="rk", body=b"hello", mandatory=True)
        transport.publish(envelope)

        channel.confirm_delivery.assert_not_called()

    def test_publish_unroutable_mandatory_returns_returned_status(self) -> None:
        """H1: an UnroutableError (broker returned the message — no matching
        binding) must map to PublishStatus.RETURNED, not the generic ERROR,
        so outcome.ok is False and callers get an actionable status."""
        import pika.exceptions

        transport = _make_transport()
        channel = self._connect_transport(transport)
        channel.basic_publish.side_effect = pika.exceptions.UnroutableError([])

        envelope = MessageEnvelope(routing_key="rk", body=b"hello", mandatory=True)
        outcome = transport.publish(envelope)

        assert not outcome.ok
        assert outcome.status == PublishStatus.RETURNED
        assert outcome.error is not None

    def test_publish_nacked_by_broker_returns_nacked_status(self) -> None:
        """H1: a NackError (broker rejected the message, e.g. internal error)
        must map to PublishStatus.NACKED, not the generic ERROR."""
        import pika.exceptions

        transport = _make_transport()
        channel = self._connect_transport(transport)
        channel.basic_publish.side_effect = pika.exceptions.NackError([])

        envelope = MessageEnvelope(routing_key="rk", body=b"hello")
        outcome = transport.publish(envelope)

        assert not outcome.ok
        assert outcome.status == PublishStatus.NACKED
        assert outcome.error is not None

    def test_publish_mandatory_via_reply_to_channel_also_gets_confirms(self) -> None:
        """The confirm-upgrade applies to whichever channel is selected —
        including the direct reply-to channel, not just the default one."""
        transport = _make_transport()
        self._connect_transport(transport)

        reply_channel = MagicMock()
        reply_channel.is_open = True
        transport._reply_to_channel = reply_channel

        envelope = MessageEnvelope(
            routing_key="rk", body=b"hello", reply_to="amq.rabbitmq.reply-to", mandatory=True
        )
        outcome = transport.publish(envelope)

        assert outcome.ok
        reply_channel.confirm_delivery.assert_called_once()
        reply_channel.basic_publish.assert_called_once()

    def test_disconnect_clears_confirmed_channel_tracking(self) -> None:
        """The confirmed-channel-id set must be cleared on disconnect so a
        reused id (after garbage collection) can't be mistaken for an
        already-confirmed channel on the next connection."""
        transport = _make_transport(confirm_delivery=True)
        self._connect_transport(transport)
        assert transport._confirmed_channel_ids

        transport.disconnect()

        assert transport._confirmed_channel_ids == set()


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

    def test_consume_no_ack_passes_auto_ack_true(self) -> None:
        """C2: no_ack=True maps to pika's auto_ack=True (broker auto-acks)."""
        transport = _make_transport()
        channel = self._connect_transport(transport)

        transport.consume("amq.rabbitmq.reply-to", lambda msg: None, no_ack=True)

        call_kwargs = channel.basic_consume.call_args.kwargs
        assert call_kwargs["auto_ack"] is True

    def test_consume_default_no_ack_is_false(self) -> None:
        """Default consume() still uses manual ack (auto_ack=False)."""
        transport = _make_transport()
        channel = self._connect_transport(transport)

        transport.consume("orders", lambda msg: None)

        call_kwargs = channel.basic_consume.call_args.kwargs
        assert call_kwargs["auto_ack"] is False

    def test_consume_fires_channel_opened_not_rebuilt_first_time(self) -> None:
        """Item 3: the first consume() call for a queue opens a fresh
        per-queue channel -- not a rebuild."""
        transport = _make_transport()
        self._connect_transport(transport)
        opened: list[int] = []
        rebuilt: list[int] = []
        transport.on_channel_opened(lambda: opened.append(1))
        transport.on_channel_rebuilt(lambda: rebuilt.append(1))

        transport.consume("orders", lambda msg: None)

        assert opened == [1]
        assert rebuilt == []

    def test_consume_same_queue_again_fires_rebuilt(self) -> None:
        """Item 3: a second consume() call for a queue ALREADY consumed
        (the shape of _recover_consumers()'s re-subscribe-after-reconnect
        loop) is a channel rebuild, not a fresh open."""
        transport = _make_transport()
        self._connect_transport(transport)
        opened: list[int] = []
        rebuilt: list[int] = []
        transport.on_channel_opened(lambda: opened.append(1))
        transport.on_channel_rebuilt(lambda: rebuilt.append(1))

        transport.consume("orders", lambda msg: None)
        transport.consume("orders", lambda msg: None)

        assert opened == [1, 1]
        assert rebuilt == [1]

    def test_consume_declare_false_reply_to_queue_tracks_channel(self) -> None:
        """C2: consuming amq.rabbitmq.reply-to with declare=False remembers the
        channel so publish() can route matching requests onto it."""
        transport = _make_transport()
        self._connect_transport(transport)

        transport.consume("amq.rabbitmq.reply-to", lambda msg: None, no_ack=True, declare=False)

        assert transport._reply_to_channel is transport._consumer_channels["amq.rabbitmq.reply-to"]

    def test_consume_declare_true_does_not_track_reply_to_channel(self) -> None:
        """Ordinary consume() (declare=True) must not set _reply_to_channel,
        even if the queue happened to be named amq.rabbitmq.reply-to."""
        transport = _make_transport()
        self._connect_transport(transport)

        transport.consume("orders", lambda msg: None)

        assert transport._reply_to_channel is None

    def _connect_transport_with_distinct_channels(self, transport: SyncTransport) -> MagicMock:
        """Like _connect_transport, but self._connection.channel() returns a
        FRESH mock on each call — required to verify that publish() picks
        the correct one of several distinct channels (channel-affinity)."""
        publisher_channel = MagicMock()
        publisher_channel.is_open = True
        call_count = [0]

        def channel_side_effect(*args: object, **kwargs: object) -> MagicMock:
            call_count[0] += 1
            if call_count[0] == 1:
                return publisher_channel
            ch = MagicMock()
            ch.is_open = True
            return ch

        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_conn.return_value.channel.side_effect = channel_side_effect
                mock_conn.return_value.is_open = True
                transport.connect()

        return publisher_channel

    def test_publish_routes_reply_to_request_onto_reply_channel(self) -> None:
        """C2: a publish with reply_to=amq.rabbitmq.reply-to must use the SAME
        channel that registered the reply consumer, not the default publisher
        channel — RabbitMQ rejects the request otherwise (channel affinity)."""
        transport = _make_transport()
        publisher_channel = self._connect_transport_with_distinct_channels(transport)

        transport.consume("amq.rabbitmq.reply-to", lambda msg: None, no_ack=True, declare=False)
        reply_channel = transport._reply_to_channel
        assert reply_channel is not publisher_channel

        outcome = transport.publish(
            MessageEnvelope(routing_key="rpc.q", body=b"req", reply_to="amq.rabbitmq.reply-to")
        )

        assert outcome.ok
        reply_channel.basic_publish.assert_called_once()
        publisher_channel.basic_publish.assert_not_called()

    def test_publish_without_reply_to_uses_default_channel(self) -> None:
        """A normal publish (no direct reply-to) must still use the default
        publisher channel, even while a reply-to consumer is active."""
        transport = _make_transport()
        publisher_channel = self._connect_transport_with_distinct_channels(transport)

        transport.consume("amq.rabbitmq.reply-to", lambda msg: None, no_ack=True, declare=False)

        outcome = transport.publish(MessageEnvelope(routing_key="orders", body=b"data"))

        assert outcome.ok
        publisher_channel.basic_publish.assert_called_once()
        transport._reply_to_channel.basic_publish.assert_not_called()

    def test_cancel_consumer_clears_reply_to_channel(self) -> None:
        """Cancelling the reply-to consumer must clear _reply_to_channel so a
        later publish falls back to the default channel instead of a closed one."""
        transport = _make_transport()
        self._connect_transport(transport)

        tag = transport.consume("amq.rabbitmq.reply-to", lambda msg: None, no_ack=True, declare=False)
        assert transport._reply_to_channel is not None

        transport.cancel_consumer(tag)

        assert transport._reply_to_channel is None


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

    def test_build_message_converts_timestamp(self) -> None:
        """Regression: pika's int timestamp is surfaced as a tz-aware datetime
        (msg.timestamp used to always be None on consume)."""
        from datetime import UTC, datetime

        transport = _make_transport()
        method = MagicMock()
        method.routing_key = "q"
        method.exchange = ""
        method.delivery_tag = 1
        method.redelivered = False
        method.consumer_tag = "t"
        props = MagicMock()
        props.headers = None
        props.timestamp = 1704164645  # 2024-01-02T03:04:05Z

        msg = transport._build_message(MagicMock(), method, props, b"{}")

        assert msg.timestamp == datetime.fromtimestamp(1704164645, tz=UTC)

    def test_build_message_surfaces_priority_expiration_user_id(self) -> None:
        """priority/expiration/user_id used to be dropped entirely on consume
        (RabbitMessage had no slots for them), so a retry/DLQ-replay envelope
        built from a consumed message could never carry them forward."""
        transport = _make_transport()
        method = MagicMock()
        method.routing_key = "q"
        method.exchange = ""
        method.delivery_tag = 1
        method.redelivered = False
        method.consumer_tag = "t"
        props = MagicMock()
        props.headers = None
        props.timestamp = None
        props.priority = 5
        props.expiration = "60000"
        props.user_id = "guest"

        msg = transport._build_message(MagicMock(), method, props, b"{}")

        assert msg.priority == 5
        assert msg.expiration == "60000"
        assert msg.user_id == "guest"

    def test_build_message_no_ack_skips_settlement_wiring(self) -> None:
        """C2: a no-ack delivery (e.g. amq.rabbitmq.reply-to) gets no ack/nack/
        reject functions — the broker already auto-acked it, and calling
        basic_ack/nack/reject on such a delivery would be a protocol violation."""
        transport = _make_transport()
        method = MagicMock()
        method.routing_key = "amq.rabbitmq.reply-to"
        method.exchange = ""
        method.delivery_tag = 1
        method.redelivered = False
        method.consumer_tag = "rpc-tag"
        props = MagicMock()
        props.headers = None
        props.timestamp = None

        msg = transport._build_message(MagicMock(), method, props, b"reply-body", no_ack=True)

        assert msg._ack_fn is None
        assert msg._nack_fn is None
        assert msg._reject_fn is None
        with pytest.raises(RuntimeError):
            msg.ack()

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
            # New start_consuming drives the connection I/O loop via process_data_events.
            transport._connection.process_data_events.side_effect = KeyboardInterrupt()

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


# -- R-3: _run_on_io_thread zombie callback is a no-op after a stall ---------


class TestRunOnIoThreadStall:
    """R-3: when the I/O loop stalls and the wait times out, a late callback
    drain must be a no-op (cancelled flag set) so it cannot settle an
    already-redelivered message."""

    def test_stalled_callback_is_noop_after_timeout(self) -> None:
        transport = _make_transport()
        # Force the cross-thread path: a consume loop has run, different owner.
        transport._consuming = True
        transport._ever_consumed = True
        transport._owner_ident = -99999

        queued: list = []
        conn = MagicMock()
        conn.add_callback_threadsafe.side_effect = lambda cb: queued.append(cb)
        transport._connection = conn

        sentinel: list[str] = []

        def fn() -> None:
            sentinel.append("ran")

        with pytest.raises(TimeoutError):
            transport._run_on_io_thread(fn, timeout=0.05)

        # The callback was queued but never drained within the timeout.
        assert sentinel == []
        assert len(queued) == 1

        # Simulate the late drain (the I/O loop finally runs the callback).
        # R-3: the cancelled flag must make this a no-op so fn() does NOT run.
        queued[0]()
        assert sentinel == []  # fn() was NOT executed by the late callback

    def test_inline_path_runs_fn_immediately(self) -> None:
        """On the owner thread, _run_on_io_thread runs fn() inline (no marshal)."""
        import threading

        transport = _make_transport()
        transport._consuming = True
        transport._owner_ident = threading.get_ident()  # same thread -> inline
        transport._connection = MagicMock()

        ran: list[bool] = []
        transport._run_on_io_thread(lambda: ran.append(True))
        assert ran == [True]


# -- I-10: publish respects confirm_timeout on a stalled I/O loop ------------


class TestPublishConfirmTimeout:
    """I-10: a publish whose confirm never arrives (stalled I/O loop) raises
    within ~confirm_timeout instead of hanging the worker forever."""

    def test_publish_raises_within_confirm_timeout(self) -> None:
        pytest.importorskip("pika")
        import time

        transport = _make_transport(confirm_timeout=0.1)
        # Make the transport appear connected and force the cross-thread path
        # so the publish marshal is bounded by confirm_timeout.
        transport._connected = True
        transport._consuming = True
        transport._ever_consumed = True
        transport._owner_ident = -99999

        channel = MagicMock()
        channel.is_open = True
        transport._channel = channel

        conn = MagicMock()
        conn.is_open = True
        # The I/O loop never drains the callback -> publish marshal times out.
        conn.add_callback_threadsafe.side_effect = lambda cb: None
        transport._connection = conn

        env = MessageEnvelope(routing_key="rk", body=b"x")
        start = time.monotonic()
        outcome = transport.publish(env)
        elapsed = time.monotonic() - start

        # A confirm that never arrives is exactly the documented TIMEOUT case
        # (docs/message-safety.md), matching the async transport's equivalent
        # asyncio.timeout(confirm_timeout) branch -- not a generic ERROR.
        assert outcome.status == PublishStatus.TIMEOUT
        assert isinstance(outcome.error, TimeoutError)
        # Bounded by ~confirm_timeout (allow scheduling slack, but well under 30s).
        assert elapsed < 1.0

    def test_confirm_timeout_kwarg_stored(self) -> None:
        transport = _make_transport(confirm_timeout=7.5)
        assert transport._confirm_timeout == 7.5

    def test_confirm_timeout_default(self) -> None:
        transport = _make_transport()
        assert transport._confirm_timeout == 5.0


class TestPublishConfirmWaitBoundedInline:
    """I-11: pika's BlockingChannel.basic_publish() has no timeout
    parameter -- when _run_on_io_thread would otherwise run it fully
    inline and unbounded (pure producer / nothing has ever consumed on
    this connection), a wedged broker (confirms never arrive) used to hang
    the calling thread forever, confirm_timeout notwithstanding. The
    confirm wait is now bounded via a dedicated helper thread, and a
    timeout poisons the connection so the next call reconnects instead of
    reusing one a background thread might still be touching."""

    def test_pure_producer_publish_times_out_instead_of_hanging(self) -> None:
        import threading
        import time

        transport = _make_transport(confirm_timeout=0.1)
        transport._connected = True
        transport._ever_consumed = False  # pure producer: the unbounded case
        transport._owner_ident = threading.get_ident()  # same thread as caller

        channel = MagicMock()
        channel.is_open = True
        channel.basic_publish.side_effect = lambda **kw: time.sleep(100)
        transport._channel = channel

        conn = MagicMock()
        conn.is_open = True
        transport._connection = conn

        env = MessageEnvelope(routing_key="rk", body=b"x")
        start = time.monotonic()
        outcome = transport.publish(env)
        elapsed = time.monotonic() - start

        assert outcome.status == PublishStatus.TIMEOUT
        assert isinstance(outcome.error, TimeoutError)
        assert elapsed < 1.0

    def test_timeout_poisons_connection_for_reconnect(self) -> None:
        import threading
        import time

        transport = _make_transport(confirm_timeout=0.1)
        transport._connected = True
        transport._ever_consumed = False
        transport._owner_ident = threading.get_ident()

        channel = MagicMock()
        channel.is_open = True
        channel.basic_publish.side_effect = lambda **kw: time.sleep(100)
        transport._channel = channel

        conn = MagicMock()
        conn.is_open = True
        transport._connection = conn

        env = MessageEnvelope(routing_key="rk", body=b"x")
        transport.publish(env)

        assert transport._connection is None
        assert transport._channel is None
        assert transport._connected is False
        assert transport._owner_ident is None

    def test_pure_producer_publish_succeeds_when_confirm_arrives_promptly(self) -> None:
        """The common case -- a confirm that arrives quickly still works
        correctly through the new bounded-helper-thread path."""
        import threading

        transport = _make_transport(confirm_timeout=5.0)
        transport._connected = True
        transport._ever_consumed = False
        transport._owner_ident = threading.get_ident()

        channel = MagicMock()
        channel.is_open = True
        transport._channel = channel

        conn = MagicMock()
        conn.is_open = True
        transport._connection = conn

        env = MessageEnvelope(routing_key="rk", body=b"x")
        outcome = transport.publish(env)

        assert outcome.status == PublishStatus.CONFIRMED
        channel.basic_publish.assert_called_once()
        # Not poisoned -- the connection is still usable.
        assert transport._connection is conn

    def test_actively_consuming_same_thread_runs_unbounded_inline(self) -> None:
        """Documented residual limitation (see _publish_on_channel): cannot
        safely bound this case, since this thread also drives
        start_consuming()'s dispatch loop on the same connection. Verify
        behavior is unchanged -- basic_publish is called directly, no
        helper thread involved."""
        import threading

        transport = _make_transport(confirm_timeout=5.0)
        transport._connected = True
        transport._ever_consumed = True
        transport._owner_ident = threading.get_ident()

        channel = MagicMock()
        channel.is_open = True
        transport._channel = channel

        conn = MagicMock()
        conn.is_open = True
        transport._connection = conn

        env = MessageEnvelope(routing_key="rk", body=b"x")
        outcome = transport.publish(env)

        channel.basic_publish.assert_called_once()
        assert outcome.status == PublishStatus.CONFIRMED


# ── I-17: cross-thread stop_consuming marshalling ──────────────────────────


class TestStopConsumingCrossThread:
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

    def test_stop_consuming_inline_when_not_consuming(self) -> None:
        """When not in a consume loop, stop_consuming runs inline (synchronous)."""
        transport = _make_transport()
        channel = self._connect_transport(transport)
        transport.stop_consuming()
        channel.stop_consuming.assert_called_once()

    def test_stop_consuming_marshals_cross_thread_during_consume(self) -> None:
        """During active consuming on a different thread, stop_consuming marshals
        via add_callback_threadsafe (I-17) instead of calling pika cross-thread."""
        transport = _make_transport()
        channel = self._connect_transport(transport)
        # Pretend an active consume loop owned by a different thread.
        transport._consuming = True
        transport._ever_consumed = True
        transport._owner_ident = 1  # some other thread id
        marshalled: list = []
        conn = transport._connection
        # Simulate the I/O loop draining the callback synchronously.
        conn.add_callback_threadsafe = lambda cb: (marshalled.append(cb), cb())[1]
        transport.stop_consuming()
        # The call was marshalled via add_callback_threadsafe (not inline).
        assert len(marshalled) == 1
        # The drained callback performed the actual stop on the I/O thread.
        channel.stop_consuming.assert_called_once()


# ── on_blocked / on_unblocked callbacks ──────────────────────────────────


class TestBlockedUnblockedCallbacksSync:
    def test_on_blocked_registers_callback(self) -> None:
        """Line 95: on_blocked() appends callback to _blocked_callbacks."""
        transport = _make_transport()

        def cb() -> None:
            pass

        transport.on_blocked(cb)
        assert cb in transport._blocked_callbacks

    def test_on_unblocked_registers_callback(self) -> None:
        """Line 99: on_unblocked() appends callback to _unblocked_callbacks."""
        transport = _make_transport()

        def cb() -> None:
            pass

        transport.on_unblocked(cb)
        assert cb in transport._unblocked_callbacks

    def test_pika_blocked_calls_all_callbacks(self) -> None:
        """Lines 102-104: _pika_blocked() calls every registered blocked callback."""
        transport = _make_transport()
        called: list[str] = []
        transport.on_blocked(lambda: called.append("cb1"))
        transport.on_blocked(lambda: called.append("cb2"))
        transport._pika_blocked(None)
        assert called == ["cb1", "cb2"]

    def test_pika_unblocked_calls_all_callbacks(self) -> None:
        """Lines 109-111: _pika_unblocked() calls every registered unblocked callback."""
        transport = _make_transport()
        called: list[str] = []
        transport.on_unblocked(lambda: called.append("cb1"))
        transport.on_unblocked(lambda: called.append("cb2"))
        transport._pika_unblocked(None)
        assert called == ["cb1", "cb2"]

    def test_pika_blocked_no_callbacks_is_noop(self) -> None:
        """_pika_blocked() with no callbacks does nothing."""
        transport = _make_transport()
        transport._pika_blocked(None)  # should not raise

    def test_pika_unblocked_no_callbacks_is_noop(self) -> None:
        """_pika_unblocked() with no callbacks does nothing."""
        transport = _make_transport()
        transport._pika_unblocked(None)  # should not raise

    def test_is_blocked_tracked_without_any_callback(self) -> None:
        """L15: is_blocked reflects connection.blocked/unblocked frames even
        with zero on_blocked/on_unblocked callbacks registered -- health.py
        reads this directly when no FlowController is wired."""
        transport = _make_transport()
        assert transport.is_blocked is False

        transport._pika_blocked(None)
        assert transport.is_blocked is True

        transport._pika_unblocked(None)
        assert transport.is_blocked is False


# ── sync context manager ──────────────────────────────────────────────────


class TestSyncContextManager:
    @pytest.fixture(autouse=True)
    def _check_pika(self) -> None:
        pytest.importorskip("pika")

    def _make_connected_mock(self):
        mock_channel = MagicMock()
        mock_channel.is_open = True
        mock_conn = MagicMock()
        mock_conn.is_open = True
        mock_conn.channel.return_value = mock_channel
        return mock_conn, mock_channel

    def test_enter_connects_and_returns_self(self) -> None:
        """Lines 158-159: __enter__ calls connect() and returns self."""
        transport = _make_transport()
        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_bc:
                mock_conn, _mock_ch = self._make_connected_mock()
                mock_bc.return_value = mock_conn
                result = transport.__enter__()
        assert result is transport
        assert transport.is_connected()

    def test_exit_disconnects(self) -> None:
        """Line 162: __exit__ calls disconnect()."""
        transport = _make_transport()
        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_bc:
                mock_conn, _mock_ch = self._make_connected_mock()
                mock_bc.return_value = mock_conn
                transport.connect()
        assert transport.is_connected()
        transport.__exit__(None, None, None)
        assert not transport.is_connected()


# ── disconnect consumer channels ──────────────────────────────────────────


class TestDisconnectConsumerChannelsSync:
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

    def test_disconnect_closes_open_consumer_channels(self) -> None:
        """Lines 172-174: disconnect() closes each open consumer channel."""
        transport = _make_transport()
        self._connect_transport(transport)

        consumer_ch = MagicMock()
        consumer_ch.is_open = True
        transport._consumer_channels["q1"] = consumer_ch

        transport.disconnect()

        consumer_ch.close.assert_called_once()

    def test_disconnect_skips_closed_consumer_channels(self) -> None:
        """Lines 172-174: disconnect() skips already-closed consumer channels."""
        transport = _make_transport()
        self._connect_transport(transport)

        closed_ch = MagicMock()
        closed_ch.is_open = False
        transport._consumer_channels["q1"] = closed_ch

        transport.disconnect()

        closed_ch.close.assert_not_called()


# ── has_open_channels ────────────────────────────────────────────────────


class TestHasOpenChannelsSync:
    def test_has_open_channels_false_when_empty(self) -> None:
        """Lines 218-219: returns False when no consumer channels registered."""
        transport = _make_transport()
        assert transport.has_open_channels is False

    def test_has_open_channels_true_when_all_open(self) -> None:
        """Returns True when all channels are open."""
        transport = _make_transport()
        ch = MagicMock()
        ch.is_open = True
        transport._consumer_channels["q1"] = ch
        assert transport.has_open_channels is True

    def test_has_open_channels_false_when_any_closed(self) -> None:
        """Returns False when at least one channel is closed."""
        transport = _make_transport()
        open_ch = MagicMock()
        open_ch.is_open = True
        closed_ch = MagicMock()
        closed_ch.is_open = False
        transport._consumer_channels["q1"] = open_ch
        transport._consumer_channels["q2"] = closed_ch
        assert transport.has_open_channels is False


# ── _ensure_connected exhaustion ─────────────────────────────────────────


class TestEnsureConnectedExhaustion:
    def test_ensure_connected_raises_after_max_attempts(self) -> None:
        """Lines 261-266: _ensure_connected() re-raises after max attempts."""
        pytest.importorskip("pika")
        import pika

        transport = _make_transport()
        transport.max_reconnect_attempts = 1

        with patch.object(transport, "connect", side_effect=pika.exceptions.AMQPConnectionError("down")):
            with patch("rabbitkit.sync.transport.time.sleep"):
                with pytest.raises(pika.exceptions.AMQPConnectionError):
                    transport._ensure_connected()

    def test_ensure_connected_raises_after_deadline(self) -> None:
        """Lines 260-266: _ensure_connected() re-raises after time deadline."""
        pytest.importorskip("pika")
        import pika

        transport = _make_transport()
        # Make the deadline already past on first check
        transport._reconnect_total_timeout = 0.0

        with patch.object(transport, "connect", side_effect=pika.exceptions.AMQPConnectionError("down")):
            with patch("rabbitkit.sync.transport.time.sleep"):
                with pytest.raises(pika.exceptions.AMQPConnectionError):
                    transport._ensure_connected()


# ── _run_on_io_thread error propagation ──────────────────────────────────


class TestRunOnIoThreadErrors:
    def test_run_on_io_thread_propagates_exception_from_cb(self) -> None:
        """Lines 310-311: _run_on_io_thread() re-raises exception from the fn."""
        transport = _make_transport()
        transport._consuming = True
        transport._ever_consumed = True
        transport._owner_ident = -99999  # different thread

        conn = MagicMock()

        def drain_immediately(cb):
            cb()  # run the callback inline to simulate I/O loop draining it

        conn.add_callback_threadsafe.side_effect = drain_immediately
        transport._connection = conn

        class _Boom(Exception):
            pass

        def boom() -> None:
            raise _Boom("intentional error")

        with pytest.raises(_Boom, match="intentional error"):
            transport._run_on_io_thread(boom)

    def test_run_on_io_thread_returns_result_from_cb(self) -> None:
        """Line 329-330: _run_on_io_thread() returns fn() result on cross-thread path."""
        transport = _make_transport()
        transport._consuming = True
        transport._ever_consumed = True
        transport._owner_ident = -99999

        conn = MagicMock()

        def drain_immediately(cb):
            cb()

        conn.add_callback_threadsafe.side_effect = drain_immediately
        transport._connection = conn

        result = transport._run_on_io_thread(lambda: 42)
        assert result == 42


# ── cancel_consumer: tag not found ───────────────────────────────────────


class TestCancelConsumerTagNotFound:
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

    def test_cancel_consumer_unknown_tag_is_noop(self) -> None:
        """Line 512: cancel_consumer() returns when tag is not in consumer_tags."""
        transport = _make_transport()
        self._connect_transport(transport)
        # Tag not registered — should not raise
        transport.cancel_consumer("rabbitkit.nonexistent-tag")


# ── start_consuming loop ──────────────────────────────────────────────────


class TestStartConsuming:
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

    def test_start_consuming_exits_when_no_consumer_channels(self) -> None:
        """Lines 538-555: start_consuming() breaks when _consumer_channels is empty."""
        transport = _make_transport()
        self._connect_transport(transport)

        # No consumer channels registered — loop should break after first iteration
        call_count = 0

        def process_one(time_limit: float) -> None:
            nonlocal call_count
            call_count += 1

        transport._connection.process_data_events = process_one

        transport.start_consuming()

        assert call_count == 1  # one call before break
        assert transport._consuming is False

    def test_start_consuming_keyboard_interrupt_calls_stop(self) -> None:
        """Lines 552-553: KeyboardInterrupt triggers _stop_all_consumers()."""
        transport = _make_transport()
        self._connect_transport(transport)

        transport._connection.process_data_events.side_effect = KeyboardInterrupt()

        transport.start_consuming()

        # _stop_all_consumers should have been called; consuming is False
        assert transport._consuming is False

    def test_start_consuming_processes_until_consumers_cleared(self) -> None:
        """Lines 542-551: loop calls process_data_events while channels exist."""
        transport = _make_transport()
        self._connect_transport(transport)

        # Add a consumer channel that disappears after first process_data_events
        consumer_ch = MagicMock()
        consumer_ch.is_open = True
        transport._consumer_channels["q1"] = consumer_ch

        call_count = 0

        def remove_after_first(time_limit: float) -> None:
            nonlocal call_count
            call_count += 1
            transport._consumer_channels.clear()

        transport._connection.process_data_events = remove_after_first

        transport.start_consuming()

        assert call_count == 1
        assert transport._consuming is False

    def test_on_io_tick_fires_once_per_loop_iteration(self) -> None:
        """L14: on_io_tick callbacks fire once per process_data_events() call,
        not once per delivered message -- the liveness-heartbeat hook."""
        transport = _make_transport()
        self._connect_transport(transport)

        consumer_ch = MagicMock()
        consumer_ch.is_open = True
        transport._consumer_channels["q1"] = consumer_ch

        tick_count = 0

        def process_twice(time_limit: float) -> None:
            nonlocal tick_count
            if tick_count >= 1:
                transport._consumer_channels.clear()

        transport._connection.process_data_events = process_twice

        def on_tick() -> None:
            nonlocal tick_count
            tick_count += 1

        transport.on_io_tick(on_tick)
        transport.start_consuming()

        assert tick_count == 2  # two loop iterations before the channel clears

    def test_on_io_tick_callback_exception_does_not_break_loop(self) -> None:
        """L14: a raising io_tick callback is caught -- never breaks the I/O loop."""
        transport = _make_transport()
        self._connect_transport(transport)

        consumer_ch = MagicMock()
        consumer_ch.is_open = True
        transport._consumer_channels["q1"] = consumer_ch

        def process_one(time_limit: float) -> None:
            transport._consumer_channels.clear()

        transport._connection.process_data_events = process_one

        def bad_tick() -> None:
            raise RuntimeError("boom")

        transport.on_io_tick(bad_tick)
        transport.start_consuming()  # must not raise

        assert transport._consuming is False


# ── stop_consuming: not connected / timeout ───────────────────────────────


class TestStopConsumingEdgeCases:
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

    def test_stop_consuming_when_not_connected_is_noop(self) -> None:
        """Line 583: stop_consuming() returns immediately when not connected."""
        transport = _make_transport()
        # Never connected — should not raise
        transport.stop_consuming()

    def test_stop_consuming_timeout_is_logged_not_raised(self) -> None:
        """Lines 586-590: TimeoutError from _run_on_io_thread is caught and logged."""
        transport = _make_transport()
        self._connect_transport(transport)

        with patch.object(transport, "_run_on_io_thread", side_effect=TimeoutError("stalled")):
            # Should not raise even though _run_on_io_thread times out
            transport.stop_consuming()


# ── basic_get / purge_queue (sync) ────────────────────────────────────────


class TestBasicGetAndPurgeSync:
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

    def test_basic_get_returns_none_when_queue_empty(self) -> None:
        """Lines 599-602: basic_get() returns None when method is None."""
        transport = _make_transport()
        channel = self._connect_transport(transport)

        # basic_get returns (None, None, None) for an empty queue
        channel.basic_get.return_value = (None, None, None)

        result = transport.basic_get("orders.dlq")
        assert result is None

    def test_basic_get_returns_message_when_present(self) -> None:
        """Line 603: basic_get() builds and returns a RabbitMessage."""
        transport = _make_transport()
        channel = self._connect_transport(transport)

        mock_method = MagicMock()
        mock_method.routing_key = "orders.dlq"
        mock_method.exchange = ""
        mock_method.delivery_tag = 7
        mock_method.redelivered = False
        mock_method.consumer_tag = None

        mock_props = MagicMock()
        mock_props.headers = None
        mock_props.timestamp = None
        mock_props.message_id = None
        mock_props.correlation_id = None
        mock_props.reply_to = None
        mock_props.content_type = None
        mock_props.content_encoding = None
        mock_props.type = None
        mock_props.app_id = None

        channel.basic_get.return_value = (mock_method, mock_props, b"dlq-payload")

        result = transport.basic_get("orders.dlq")
        assert result is not None
        assert result.body == b"dlq-payload"
        assert result.delivery_tag == 7

    def test_purge_queue_returns_message_count(self) -> None:
        """Lines 607-609: purge_queue() returns the message count from frame."""
        transport = _make_transport()
        channel = self._connect_transport(transport)

        purge_frame = MagicMock()
        purge_frame.method.message_count = 15
        channel.queue_purge.return_value = purge_frame

        count = transport.purge_queue("orders.dlq")
        assert count == 15
        channel.queue_purge.assert_called_once_with(queue="orders.dlq")


# ── reconnect() ──────────────────────────────────────────────────────────


class TestReconnect:
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

    def test_reconnect_disconnects_then_reconnects(self) -> None:
        """Lines 270-271: reconnect() calls disconnect() then _ensure_connected()."""
        transport = _make_transport()
        self._connect_transport(transport)
        assert transport.is_connected()

        disconnect_called: list[bool] = []
        ensure_called: list[bool] = []

        original_disconnect = transport.disconnect

        def spy_disconnect() -> None:
            disconnect_called.append(True)
            original_disconnect()

        def spy_ensure() -> None:
            ensure_called.append(True)
            # Don't actually reconnect in this test

        transport.disconnect = spy_disconnect  # type: ignore[method-assign]
        transport._ensure_connected = spy_ensure  # type: ignore[method-assign]

        transport.reconnect()

        assert disconnect_called == [True]
        assert ensure_called == [True]


class TestEnsureConnectedPublicWrapper:
    """ensure_connected(): idle-pump support -- unlike reconnect(), a no-op
    when already connected; only reconnects if actually dead."""

    def test_noop_when_already_connected(self) -> None:
        transport = _make_transport()
        mock_channel = MagicMock()
        mock_channel.is_open = True
        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                transport.connect()

        with patch("pika.BlockingConnection") as mock_conn_2:
            transport.ensure_connected()

        mock_conn_2.assert_not_called()

    def test_delegates_to_ensure_connected(self) -> None:
        transport = _make_transport()
        calls: list[bool] = []
        transport._ensure_connected = lambda: calls.append(True)  # type: ignore[method-assign]

        transport.ensure_connected()

        assert calls == [True]


# ── H2: pump() ────────────────────────────────────────────────────────────


class TestPump:
    def test_pump_drives_connection_when_open(self) -> None:
        """pump() calls process_data_events on an open connection."""
        transport = _make_transport()
        conn = MagicMock()
        conn.is_open = True
        transport._connection = conn

        transport.pump(0.2)

        conn.process_data_events.assert_called_once_with(time_limit=0.2)

    def test_pump_default_time_limit(self) -> None:
        transport = _make_transport()
        conn = MagicMock()
        conn.is_open = True
        transport._connection = conn

        transport.pump()

        conn.process_data_events.assert_called_once_with(time_limit=0.05)

    def test_pump_noop_when_no_connection(self) -> None:
        transport = _make_transport()
        transport._connection = None
        transport.pump()  # must not raise

    def test_pump_noop_when_connection_closed(self) -> None:
        transport = _make_transport()
        conn = MagicMock()
        conn.is_open = False
        transport._connection = conn

        transport.pump()

        conn.process_data_events.assert_not_called()


# ── H2: _ever_consumed gates cross-thread marshaling, not _consuming ──────


class TestEverConsumedGatesMarshaling:
    """H2: a worker thread's ack/nack/reject must marshal onto the owner
    thread whenever a consume loop has EVER run on this connection — even
    after the loop has stopped pumping (_consuming=False), which is exactly
    the state during SyncBroker.stop()'s worker-pool drain. Before this fix,
    _run_on_io_thread fell back to running fn() INLINE on the calling
    (worker) thread whenever `not self._consuming`, regardless of whether a
    consume loop had ever run — an unsynchronized cross-thread pika call.
    """

    def test_marshals_when_ever_consumed_even_if_not_currently_consuming(self) -> None:
        """The core H2 regression check: consuming has stopped (shutdown),
        but a consume loop DID run at some point — a cross-thread call must
        still marshal via add_callback_threadsafe, not run inline."""
        transport = _make_transport()
        transport._ever_consumed = True
        transport._consuming = False  # loop has stopped, e.g. mid-drain
        transport._owner_ident = -99999  # calling thread is NOT the owner

        conn = MagicMock()
        marshalled: list = []

        def drain_immediately(cb):
            marshalled.append(cb)
            cb()

        conn.add_callback_threadsafe.side_effect = drain_immediately
        transport._connection = conn

        ran_on_thread: list[int] = []

        def ack_fn() -> None:
            import threading

            ran_on_thread.append(threading.get_ident())

        transport._run_on_io_thread(ack_fn)

        # Must have gone through the marshal path (add_callback_threadsafe),
        # not run inline directly on the caller.
        assert len(marshalled) == 1

    def test_runs_inline_when_never_consumed_regardless_of_thread(self) -> None:
        """No consume loop has ever run on this connection (pure producer) —
        nothing else drives the socket concurrently, so a cross-thread call
        is safe to run inline (and necessary: nothing pumps to drain a
        marshaled callback in this mode)."""
        transport = _make_transport()
        transport._ever_consumed = False
        transport._consuming = False
        transport._owner_ident = -99999  # different thread, never consumed

        conn = MagicMock()
        transport._connection = conn

        ran: list[bool] = []
        transport._run_on_io_thread(lambda: ran.append(True))

        assert ran == [True]
        conn.add_callback_threadsafe.assert_not_called()

    def test_marshaled_ack_from_worker_thread_runs_on_owner_thread(self) -> None:
        """Instrument threading.get_ident() in an ack-like callable submitted
        from a real background (worker) thread: once a consume loop has run,
        the callable must execute on the connection's owner thread identity,
        never on the calling worker thread — even though _consuming is False
        at call time (post-shutdown drain window)."""
        import threading

        transport = _make_transport()
        owner_ident = threading.get_ident()
        transport._ever_consumed = True
        transport._consuming = False
        transport._owner_ident = owner_ident

        conn = MagicMock()
        callback_queue: queue.Queue = queue.Queue()
        # Simulate add_callback_threadsafe's real contract: it only QUEUES the
        # callback for the owner thread to run later — it does NOT run it on
        # the calling thread. The test drives the drain itself (below) from
        # the main/owner thread, exactly like SyncTransport.pump() would.
        conn.add_callback_threadsafe.side_effect = callback_queue.put
        transport._connection = conn

        recorded_idents: list[int] = []

        def ack_fn() -> None:
            recorded_idents.append(threading.get_ident())

        worker_exceptions: list[BaseException] = []

        def worker() -> None:
            try:
                transport._run_on_io_thread(ack_fn)
            except BaseException as exc:  # pragma: no cover - surfaced via list
                worker_exceptions.append(exc)

        t = threading.Thread(target=worker)
        t.start()

        # Act as the owner thread: drain the marshaled callback ourselves.
        cb = callback_queue.get(timeout=5.0)
        cb()

        t.join(timeout=5.0)

        assert not worker_exceptions
        # ack_fn ran on THIS (owner) thread, never on the worker thread that
        # called _run_on_io_thread — the exact H2 invariant.
        assert recorded_idents == [owner_ident]
        assert recorded_idents[0] != t.ident


# ── H2: NackError/UnroutableError propagate across the marshal boundary ───


class TestPublishCrossThreadConfirmPropagation:
    """H2: pika's BlockingChannel.basic_publish() (confirm mode) blocks and
    positively asserts the confirm frame before returning normally — it
    raises NackError/UnroutableError instead of returning when the broker
    nacks or returns the message (verified directly against pika's source:
    ``assert isinstance(conf_method, pika.spec.Basic.Ack)`` before a plain
    return). This class proves that guarantee survives unchanged across
    ``_run_on_io_thread``'s cross-thread marshal path: an exception raised by
    ``fn()`` on the owner thread must propagate back to the calling (worker)
    thread and map to NACKED/RETURNED — never silently reported as
    CONFIRMED just because no exception surfaced on the *caller's* thread."""

    def _connect_transport(self, transport: SyncTransport) -> MagicMock:
        mock_channel = MagicMock()
        mock_channel.is_open = True
        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                transport.connect()
        return mock_channel

    def _force_marshal_path(self, transport: SyncTransport) -> None:
        import threading

        transport._ever_consumed = True
        transport._owner_ident = threading.get_ident()  # "owner" == main thread
        conn = transport._connection
        # Drain synchronously so the test stays deterministic; what matters
        # here is that the call went through the marshal machinery (_cb,
        # done.wait(), error[]/raise) rather than the inline early-return.
        conn.add_callback_threadsafe.side_effect = lambda cb: cb()

    def test_worker_thread_publish_nacked_propagates_through_marshal(self) -> None:
        """A NackError raised by basic_publish on the marshaled path must
        surface as PublishStatus.NACKED from the calling worker thread, not
        be swallowed or misreported as CONFIRMED."""
        import threading

        import pika.exceptions

        transport = _make_transport()
        channel = self._connect_transport(transport)
        channel.basic_publish.side_effect = pika.exceptions.NackError([])
        self._force_marshal_path(transport)

        envelope = MessageEnvelope(routing_key="rk", body=b"hello")
        outcomes: list = []

        def worker() -> None:
            outcomes.append(transport.publish(envelope))

        t = threading.Thread(target=worker)
        t.start()
        t.join(timeout=5.0)

        assert len(outcomes) == 1
        assert outcomes[0].status == PublishStatus.NACKED
        assert not outcomes[0].ok

    def test_worker_thread_publish_unroutable_propagates_through_marshal(self) -> None:
        """An UnroutableError raised by basic_publish on the marshaled path
        must surface as PublishStatus.RETURNED from the calling worker
        thread."""
        import threading

        import pika.exceptions

        transport = _make_transport()
        channel = self._connect_transport(transport)
        channel.basic_publish.side_effect = pika.exceptions.UnroutableError([])
        self._force_marshal_path(transport)

        envelope = MessageEnvelope(routing_key="rk", body=b"hello", mandatory=True)
        outcomes: list = []

        def worker() -> None:
            outcomes.append(transport.publish(envelope))

        t = threading.Thread(target=worker)
        t.start()
        t.join(timeout=5.0)

        assert len(outcomes) == 1
        assert outcomes[0].status == PublishStatus.RETURNED
        assert not outcomes[0].ok

    def test_worker_thread_publish_success_is_confirmed_through_marshal(self) -> None:
        """When basic_publish raises nothing (pika already positively
        confirmed a Basic.Ack internally before returning), the marshaled
        path must still report CONFIRMED — the fix must not make every
        cross-thread publish spuriously fail."""
        import threading

        transport = _make_transport()
        channel = self._connect_transport(transport)
        channel.basic_publish.return_value = None
        self._force_marshal_path(transport)

        envelope = MessageEnvelope(routing_key="rk", body=b"hello")
        outcomes: list = []

        def worker() -> None:
            outcomes.append(transport.publish(envelope))

        t = threading.Thread(target=worker)
        t.start()
        t.join(timeout=5.0)

        assert len(outcomes) == 1
        assert outcomes[0].status == PublishStatus.CONFIRMED
        assert outcomes[0].ok


class TestStartConsumingIoLoopDeath:
    """A dead connection between poll ticks surfaces pika's internal bare
    ValueError('Timeout closed before call'); start_consuming must re-raise
    it as AMQPConnectionError so run()'s recovery loop reconnects."""

    def _wired_transport(self) -> SyncTransport:
        pytest.importorskip("pika")
        transport = _make_transport()
        mock_channel = MagicMock()
        mock_channel.is_open = True
        with patch("rabbitkit.sync.transport.make_pika_connection_params"):
            with patch("pika.BlockingConnection") as mock_conn:
                mock_conn.return_value.channel.return_value = mock_channel
                mock_conn.return_value.is_open = True
                transport.connect()
        transport._consumer_channels["q"] = mock_channel
        return transport

    def test_timeout_closed_valueerror_becomes_connection_error(self) -> None:
        import pika.exceptions

        transport = self._wired_transport()
        transport._connection.is_closed = False
        transport._connection.process_data_events.side_effect = ValueError(
            "Timeout closed before call"
        )
        with pytest.raises(pika.exceptions.AMQPConnectionError, match="connection lost mid-poll"):
            transport.start_consuming()

    def test_valueerror_on_closed_connection_becomes_connection_error(self) -> None:
        import pika.exceptions

        transport = self._wired_transport()
        transport._connection.is_closed = True
        transport._connection.process_data_events.side_effect = ValueError("anything")
        with pytest.raises(pika.exceptions.AMQPConnectionError):
            transport.start_consuming()

    def test_unrelated_valueerror_on_live_connection_reraised(self) -> None:
        transport = self._wired_transport()
        transport._connection.is_closed = False
        transport._connection.process_data_events.side_effect = ValueError("bad time_limit")
        with pytest.raises(ValueError, match="bad time_limit"):
            transport.start_consuming()


# ── Architect review: sync transport blockers ─────────────────────────────


class TestNonOwnerReconnectGuard:
    """B1 (blocker): once a consume loop has ever run, a NON-owner thread
    hitting a dead connection must get a clean ERROR outcome — never call
    connect() and steal ownership (two threads on one BlockingConnection)."""

    def test_non_owner_publish_on_dead_connection_returns_error_no_reconnect(self) -> None:
        transport = _make_transport()
        transport._connected = False  # connection died
        transport._ever_consumed = True
        transport._owner_ident = -12345  # someone else owns it

        connect_calls: list[bool] = []
        transport.connect = lambda: connect_calls.append(True)  # type: ignore[method-assign]

        outcome = transport.publish(MessageEnvelope(routing_key="q", body=b"x"))

        assert outcome.status is PublishStatus.ERROR
        assert isinstance(outcome.error, ConnectionError)
        assert connect_calls == [], "non-owner thread must never reconnect"

    def test_sticky_flag_blocks_reconnect_mid_recovery(self) -> None:
        """L1: after disconnect() reset _ever_consumed, the sticky
        _ever_consumed_any must still trip the guard for a caller that is
        not the owner thread (with _owner_ident=None, get_ident() != None
        is True for EVERY thread, so all callers are non-owners until the
        owner reconnects and reclaims it)."""
        transport = _make_transport()
        transport._ever_consumed_any = True
        transport._ever_consumed = False
        transport._owner_ident = -12345  # owner is another (reconnecting) thread
        transport._connected = False

        connect_calls: list[bool] = []
        transport.connect = lambda: connect_calls.append(True)  # type: ignore[method-assign]

        outcome = transport.publish(MessageEnvelope(routing_key="q", body=b"x"))
        assert outcome.status is PublishStatus.ERROR
        assert connect_calls == []


class TestCancelConsumerKeepsChannelOpen:
    """H1 (consumer review): cancel_consumer must basic_cancel but NOT close
    the channel — Channel.Close instantly requeues every unacked delivery
    while workers still run those handlers, and their acks then die on the
    closed channel. The channel is parked and closed by disconnect()."""

    def test_cancel_does_not_close_channel(self) -> None:
        transport = _make_transport()
        transport._connected = True
        transport._connection = MagicMock()
        transport._connection.is_open = True
        transport._channel = MagicMock()
        transport._channel.is_open = True
        channel = MagicMock()
        channel.is_open = True
        transport._consumer_channels["orders"] = channel
        transport._consumer_tags["orders"] = "ctag-1"

        transport.cancel_consumer("ctag-1")

        channel.basic_cancel.assert_called_once_with(consumer_tag="ctag-1")
        channel.close.assert_not_called()
        assert channel in transport._cancelled_channels

    def test_disconnect_closes_parked_cancelled_channels(self) -> None:
        transport = _make_transport()
        channel = MagicMock()
        channel.is_open = True
        transport._consumer_channels["orders"] = channel
        transport._consumer_tags["orders"] = "ctag-1"
        transport._connected = True
        transport._connection = MagicMock()
        transport._connection.is_open = True
        transport._channel = MagicMock()
        transport._channel.is_open = True

        transport.cancel_consumer("ctag-1")
        channel.close.assert_not_called()

        transport.disconnect()
        channel.close.assert_called_once()
        assert transport._cancelled_channels == []


class TestWarnContinueConfirmRecovery:
    """M2 (transport review): the warn_continue 406 recovery must put the
    reopened publisher channel back into confirm mode — otherwise every
    subsequent publish is fire-and-forget yet still reported CONFIRMED."""

    @staticmethod
    def _exc(code: int = 406, text: str = "inequivalent arg") -> Exception:
        class _Fake406(Exception):
            reply_code = code
            reply_text = text

        return _Fake406(text)

    def test_reopened_channel_reenters_confirm_mode(self) -> None:
        transport = _make_transport(confirm_delivery=True, on_topology_conflict="warn_continue")
        new_channel = MagicMock()
        transport._connection = MagicMock()
        transport._connection.channel.return_value = new_channel

        transport._raise_precondition_failed_or_reraise("queue", "orders", self._exc())

        new_channel.confirm_delivery.assert_called_once()
        assert transport._channel_key(new_channel) in transport._confirmed_channel_ids

    def test_reopened_channel_fires_opened_and_rebuilt(self) -> None:
        """Item 3: the 406 warn_continue reopen replaces a channel the
        broker just closed -- both channels_opened_total and
        channel_rebuilds_total must increment."""
        transport = _make_transport(confirm_delivery=True, on_topology_conflict="warn_continue")
        transport._connection = MagicMock()
        transport._connection.channel.return_value = MagicMock()
        opened: list[int] = []
        rebuilt: list[int] = []
        transport.on_channel_opened(lambda: opened.append(1))
        transport.on_channel_rebuilt(lambda: rebuilt.append(1))

        transport._raise_precondition_failed_or_reraise("queue", "orders", self._exc())

        assert opened == [1]
        assert rebuilt == [1]

    def test_conflicting_dlx_declaration_escalates_even_under_warn_continue(self) -> None:
        """Retry-review M1: a 406 on a declaration that carried an injected
        x-dead-letter-exchange must raise — continuing with the existing
        DLX-less queue silently DISCARDS every terminal reject."""
        from rabbitkit.core.errors import ConfigurationError

        transport = _make_transport(on_topology_conflict="warn_continue")
        transport._connection = MagicMock()

        with pytest.raises(ConfigurationError, match="DISCARDED"):
            transport._raise_precondition_failed_or_reraise(
                "queue",
                "orders",
                self._exc(),
                declared_arguments={"x-dead-letter-exchange": "", "x-dead-letter-routing-key": "orders.dlq"},
            )


class TestIdlePublishRetryOnce:
    """M4 (transport review): the FIRST publish after an idle heartbeat
    death must reconnect and retry exactly once (owner/pure-producer only)
    instead of returning ERROR for a message that was never sent."""

    def test_pure_producer_retries_once_after_connection_error(self) -> None:
        transport = _make_transport()
        transport._connected = True
        transport._connection = MagicMock()
        transport._connection.is_open = True
        transport._channel = MagicMock()
        transport._channel.is_open = True

        from rabbitkit.core.types import PublishOutcome
        from rabbitkit.sync.connection import get_connection_errors

        conn_err = get_connection_errors()[0]("socket died")
        attempts: list[str] = []

        def fake_publish_on_channel(channel, envelope):
            attempts.append("try")
            if len(attempts) == 1:
                return PublishOutcome(
                    status=PublishStatus.ERROR, routing_key=envelope.routing_key, error=conn_err
                )
            return PublishOutcome(status=PublishStatus.CONFIRMED, routing_key=envelope.routing_key)

        transport._publish_on_channel = fake_publish_on_channel  # type: ignore[method-assign]
        reconnects: list[bool] = []
        transport._ensure_connected = lambda: reconnects.append(True)  # type: ignore[method-assign]

        outcome = transport.publish(MessageEnvelope(routing_key="q", body=b"x"))

        assert outcome.status is PublishStatus.CONFIRMED
        assert len(attempts) == 2
        assert len(reconnects) >= 1


class TestReconnectBackoffLiveness:
    """H3 (consumer review): each reconnect-backoff iteration must fire the
    io-tick so liveness sees forward progress during a broker outage —
    otherwise every sync consumer pod restarts mid-outage."""

    def test_io_tick_fired_during_backoff(self) -> None:
        import pika

        transport = _make_transport()
        transport.max_reconnect_attempts = 2
        ticks: list[bool] = []
        transport.on_io_tick(lambda: ticks.append(True))

        def failing_connect() -> None:
            raise pika.exceptions.AMQPConnectionError("down")

        transport.connect = failing_connect  # type: ignore[method-assign]

        with patch("time.sleep"):
            with pytest.raises(pika.exceptions.AMQPConnectionError):
                transport._ensure_connected()

        assert len(ticks) >= 2  # one tick per backoff iteration


class TestEnsureConnectedOwnership:
    """Verification gap 2: ownership is enforced INSIDE _ensure_connected,
    so every entry point (publish TOCTOU, DLQInspector.basic_get,
    declare/bind helpers) is covered — a non-owner thread reaching it on a
    dead connection fails cleanly instead of creating a second
    BlockingConnection cross-thread."""

    def test_non_owner_thread_cannot_reconnect(self) -> None:
        transport = _make_transport()
        transport._connected = False
        transport._ever_consumed_any = True
        transport._owner_ident_any = -12345  # historical owner is another thread

        connect_calls: list[bool] = []
        transport.connect = lambda: connect_calls.append(True)  # type: ignore[method-assign]

        with pytest.raises(ConnectionError, match="recovery loop"):
            transport._ensure_connected()
        assert connect_calls == []

    def test_owner_thread_may_reconnect_during_recovery(self) -> None:
        import threading

        transport = _make_transport()
        transport._connected = False
        transport._ever_consumed_any = True
        transport._owner_ident_any = threading.get_ident()  # we ARE the owner

        def fake_connect() -> None:
            transport._connected = True
            transport._connection = MagicMock()
            transport._connection.is_open = True
            transport._channel = MagicMock()
            transport._channel.is_open = True

        transport.connect = fake_connect  # type: ignore[method-assign]
        transport._ensure_connected()  # must not raise
        assert transport.is_connected()

    def test_pure_producer_any_thread_may_reconnect(self) -> None:
        transport = _make_transport()
        transport._connected = False  # never consumed: no ownership

        def fake_connect() -> None:
            transport._connected = True
            transport._connection = MagicMock()
            transport._connection.is_open = True
            transport._channel = MagicMock()
            transport._channel.is_open = True

        transport.connect = fake_connect  # type: ignore[method-assign]
        transport._ensure_connected()
        assert transport.is_connected()


class TestIdlePublishRetryDeniedForNonOwner:
    """Verification gap 3: the retry-once path's False branch — a non-owner
    thread (post-consume) gets the ERROR outcome back verbatim, with no
    reconnect attempt."""

    def test_non_owner_gets_error_without_retry(self) -> None:
        from rabbitkit.core.types import PublishOutcome
        from rabbitkit.sync.connection import get_connection_errors

        transport = _make_transport()
        transport._connected = True
        transport._connection = MagicMock()
        transport._connection.is_open = True
        transport._channel = MagicMock()
        transport._channel.is_open = True
        transport._ever_consumed = True
        transport._owner_ident = -12345  # another thread owns the loop
        transport._owner_ident_any = -12345

        conn_err = get_connection_errors()[0]("socket died")
        attempts: list[str] = []

        def fake_publish_on_channel(channel, envelope):
            attempts.append("try")
            return PublishOutcome(status=PublishStatus.ERROR, routing_key=envelope.routing_key, error=conn_err)

        transport._publish_on_channel = fake_publish_on_channel  # type: ignore[method-assign]

        outcome = transport.publish(MessageEnvelope(routing_key="q", body=b"x"))
        assert outcome.status is PublishStatus.ERROR
        assert attempts == ["try"]  # exactly one attempt, no retry
