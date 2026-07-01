"""Tests for core/topology.py — Exchange and Queue models with validation."""

from __future__ import annotations

import pytest

from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import ExchangeType, QueueType

# ── RabbitExchange ────────────────────────────────────────────────────────


class TestRabbitExchange:
    def test_defaults(self) -> None:
        ex = RabbitExchange(name="orders")
        assert ex.name == "orders"
        assert ex.type == ExchangeType.DIRECT
        assert ex.durable is True
        assert ex.auto_delete is False
        assert ex.passive is False
        assert ex.internal is False
        assert ex.arguments == {}
        assert ex.bind_to is None

    def test_validate_empty_name_non_direct(self) -> None:
        # Validation now runs at construction (fail-fast __post_init__).
        with pytest.raises(ValueError, match="name"):
            RabbitExchange(name="", type=ExchangeType.FANOUT)

    def test_validate_empty_name_direct_ok(self) -> None:
        ex = RabbitExchange(name="", type=ExchangeType.DIRECT)
        ex.validate()  # should not raise

    def test_to_declare_kwargs(self) -> None:
        ex = RabbitExchange(name="events", type=ExchangeType.TOPIC, durable=True)
        kwargs = ex.to_declare_kwargs()
        assert kwargs["exchange"] == "events"
        assert kwargs["exchange_type"] == "topic"
        assert kwargs["durable"] is True
        assert kwargs["auto_delete"] is False
        assert kwargs["passive"] is False

    def test_to_bind_kwargs_no_binding(self) -> None:
        ex = RabbitExchange(name="events")
        assert ex.to_bind_kwargs() is None

    def test_to_bind_kwargs_with_binding(self) -> None:
        ex = RabbitExchange(
            name="events",
            bind_to="upstream",
            routing_key="orders.*",
            bind_arguments={"x-match": "any"},
        )
        kwargs = ex.to_bind_kwargs()
        assert kwargs is not None
        assert kwargs["destination"] == "events"
        assert kwargs["source"] == "upstream"
        assert kwargs["routing_key"] == "orders.*"
        assert kwargs["arguments"] == {"x-match": "any"}


# ── RabbitQueue — basic ──────────────────────────────────────────────────


class TestRabbitQueueBasic:
    def test_defaults(self) -> None:
        q = RabbitQueue(name="orders")
        assert q.name == "orders"
        assert q.durable is True
        assert q.exclusive is False
        assert q.queue_type == QueueType.CLASSIC
        assert q.dead_letter_exchange is None

    def test_empty_name_raises(self) -> None:
        with pytest.raises(ValueError, match="Queue name"):
            RabbitQueue(name="")

    def test_to_declare_kwargs_classic(self) -> None:
        q = RabbitQueue(name="orders", message_ttl=60000, max_length=1000)
        kwargs = q.to_declare_kwargs()
        assert kwargs["queue"] == "orders"
        assert kwargs["durable"] is True
        assert kwargs["arguments"]["x-queue-type"] == "classic"
        assert kwargs["arguments"]["x-message-ttl"] == 60000
        assert kwargs["arguments"]["x-max-length"] == 1000

    def test_to_declare_kwargs_with_dlx(self) -> None:
        q = RabbitQueue(
            name="orders",
            dead_letter_exchange="orders-dlx",
            dead_letter_routing_key="orders.dead",
        )
        kwargs = q.to_declare_kwargs()
        assert kwargs["arguments"]["x-dead-letter-exchange"] == "orders-dlx"
        assert kwargs["arguments"]["x-dead-letter-routing-key"] == "orders.dead"

    def test_to_bind_kwargs(self) -> None:
        q = RabbitQueue(name="orders", routing_key="orders.created")
        kwargs = q.to_bind_kwargs("events")
        assert kwargs["queue"] == "orders"
        assert kwargs["exchange"] == "events"
        assert kwargs["routing_key"] == "orders.created"

    def test_escape_hatch_arguments(self) -> None:
        q = RabbitQueue(name="custom", arguments={"x-custom": "value"})
        kwargs = q.to_declare_kwargs()
        assert kwargs["arguments"]["x-custom"] == "value"

    def test_escape_hatch_overrides(self) -> None:
        """User arguments override built-in ones."""
        q = RabbitQueue(
            name="custom",
            message_ttl=60000,
            arguments={"x-message-ttl": 120000},  # override
        )
        kwargs = q.to_declare_kwargs()
        assert kwargs["arguments"]["x-message-ttl"] == 120000


# ── RabbitQueue — quorum validation ──────────────────────────────────────


class TestRabbitQueueQuorum:
    def test_valid_quorum(self) -> None:
        q = RabbitQueue(name="orders", queue_type=QueueType.QUORUM)
        assert q.queue_type == QueueType.QUORUM

    def test_quorum_not_durable_raises(self) -> None:
        with pytest.raises(ValueError, match="durable"):
            RabbitQueue(name="q", queue_type=QueueType.QUORUM, durable=False)

    def test_quorum_exclusive_raises(self) -> None:
        with pytest.raises(ValueError, match="exclusive"):
            RabbitQueue(name="q", queue_type=QueueType.QUORUM, exclusive=True)

    def test_quorum_lazy_raises(self) -> None:
        with pytest.raises(ValueError, match="lazy"):
            RabbitQueue(name="q", queue_type=QueueType.QUORUM, lazy=True)

    def test_quorum_priority_raises(self) -> None:
        with pytest.raises(ValueError, match="priorities"):
            RabbitQueue(name="q", queue_type=QueueType.QUORUM, max_priority=10)

    def test_quorum_with_delivery_limit(self) -> None:
        q = RabbitQueue(name="q", queue_type=QueueType.QUORUM, delivery_limit=5)
        kwargs = q.to_declare_kwargs()
        assert kwargs["arguments"]["x-delivery-limit"] == 5

    def test_quorum_with_single_active_consumer(self) -> None:
        q = RabbitQueue(name="q", queue_type=QueueType.QUORUM, single_active_consumer=True)
        kwargs = q.to_declare_kwargs()
        assert kwargs["arguments"]["x-single-active-consumer"] is True


# ── RabbitQueue — stream validation ──────────────────────────────────────


class TestRabbitQueueStream:
    def test_valid_stream(self) -> None:
        q = RabbitQueue(name="events", queue_type=QueueType.STREAM)
        kwargs = q.to_declare_kwargs()
        assert kwargs["arguments"]["x-queue-type"] == "stream"

    def test_stream_not_durable_raises(self) -> None:
        with pytest.raises(ValueError, match="durable"):
            RabbitQueue(name="q", queue_type=QueueType.STREAM, durable=False)

    def test_stream_exclusive_raises(self) -> None:
        with pytest.raises(ValueError, match="exclusive"):
            RabbitQueue(name="q", queue_type=QueueType.STREAM, exclusive=True)

    def test_stream_lazy_raises(self) -> None:
        with pytest.raises(ValueError, match="lazy"):
            RabbitQueue(name="q", queue_type=QueueType.STREAM, lazy=True)

    def test_stream_priority_raises(self) -> None:
        with pytest.raises(ValueError, match="priorities"):
            RabbitQueue(name="q", queue_type=QueueType.STREAM, max_priority=10)

    def test_stream_message_ttl_raises(self) -> None:
        with pytest.raises(ValueError, match="TTL"):
            RabbitQueue(name="q", queue_type=QueueType.STREAM, message_ttl=60000)


# ── RabbitQueue — classic validation ─────────────────────────────────────


class TestRabbitQueueClassic:
    def test_classic_delivery_limit_raises(self) -> None:
        with pytest.raises(ValueError, match="delivery_limit"):
            RabbitQueue(name="q", queue_type=QueueType.CLASSIC, delivery_limit=5)

    def test_classic_with_lazy(self) -> None:
        q = RabbitQueue(name="q", lazy=True)
        kwargs = q.to_declare_kwargs()
        assert kwargs["arguments"]["x-queue-mode"] == "lazy"

    def test_classic_with_priority(self) -> None:
        q = RabbitQueue(name="q", max_priority=10)
        kwargs = q.to_declare_kwargs()
        assert kwargs["arguments"]["x-max-priority"] == 10


# ── RabbitQueue — warnings ───────────────────────────────────────────────


class TestRabbitQueueWarnings:
    def test_auto_delete_durable_warns(self) -> None:
        with pytest.warns(UserWarning, match="auto_delete.*durable"):
            RabbitQueue(name="q", auto_delete=True, durable=True)

    def test_passive_with_creation_options_warns(self) -> None:
        with pytest.warns(UserWarning, match="passive.*creation-only"):
            RabbitQueue(name="q", passive=True, message_ttl=60000)

    def test_passive_with_lazy_warns(self) -> None:
        with pytest.warns(UserWarning, match="passive.*creation-only"):
            RabbitQueue(name="q", passive=True, lazy=True)


# ── RabbitQueue — additional features ────────────────────────────────────


class TestRabbitQueueFeatures:
    def test_overflow(self) -> None:
        q = RabbitQueue(name="q", overflow="reject-publish")
        kwargs = q.to_declare_kwargs()
        assert kwargs["arguments"]["x-overflow"] == "reject-publish"

    def test_expires(self) -> None:
        q = RabbitQueue(name="q", expires=300000)
        kwargs = q.to_declare_kwargs()
        assert kwargs["arguments"]["x-expires"] == 300000

    def test_max_length_bytes(self) -> None:
        q = RabbitQueue(name="q", max_length_bytes=1048576)
        kwargs = q.to_declare_kwargs()
        assert kwargs["arguments"]["x-max-length-bytes"] == 1048576

    def test_bind_arguments(self) -> None:
        q = RabbitQueue(
            name="q",
            routing_key="orders.#",
            bind_arguments={"x-match": "all"},
        )
        kwargs = q.to_bind_kwargs("events")
        assert kwargs["arguments"] == {"x-match": "all"}


class TestRabbitExchangeInternalAutoDelete:
    def test_internal_and_auto_delete_raises(self) -> None:
        """RabbitExchange with internal=True and auto_delete=True is invalid."""
        from rabbitkit.core.topology import RabbitExchange
        from rabbitkit.core.types import ExchangeType
        with pytest.raises(ValueError, match="auto_delete"):
            RabbitExchange(name="x", type=ExchangeType.DIRECT, internal=True, auto_delete=True)
