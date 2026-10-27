"""middleware/metrics.py — G-22 metrics foundation: BrokerMetrics, bounded
reason codes, the metric inventory and its bounded label allow-list."""

from __future__ import annotations

import socket
import ssl
from pathlib import Path
from typing import Any

import pytest

from rabbitkit.core.config import MetricsConfig
from rabbitkit.core.errors import AuthenticationFailedError, ReconnectBudgetExhaustedError, TerminalConnectionError
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import ErrorCategory, MessageEnvelope, PublishOutcome, PublishReason, PublishStatus
from rabbitkit.middleware.metrics import (
    CONNECTION_FAILURE_REASONS,
    BrokerMetrics,
    MetricsMiddleware,
    bounded_label_values,
    connection_failure_reason,
    metric_inventory,
)

_DOCS = Path(__file__).resolve().parents[3] / "docs" / "observability.md"


class RecordingCollector:
    """Collector that records every emission (name, labels, value)."""

    def __init__(self) -> None:
        self.counters: list[tuple[str, dict[str, str], float]] = []
        self.histograms: list[tuple[str, dict[str, str], float]] = []
        self.gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self.emitted: list[tuple[str, str, dict[str, str]]] = []

    def inc_counter(self, name: str, labels: dict[str, str], value: float = 1.0) -> None:
        self.counters.append((name, dict(labels), value))
        self.emitted.append(("counter", name, dict(labels)))

    def observe_histogram(self, name: str, labels: dict[str, str], value: float) -> None:
        self.histograms.append((name, dict(labels), value))
        self.emitted.append(("histogram", name, dict(labels)))

    def set_gauge(self, name: str, labels: dict[str, str], value: float) -> None:
        self.gauges[(name, tuple(sorted(labels.items())))] = value
        self.emitted.append(("gauge", name, dict(labels)))

    def gauge(self, name: str, **labels: str) -> float | None:
        return self.gauges.get((name, tuple(sorted(labels.items()))))

    def count(self, name: str, **labels: str) -> float:
        return sum(v for n, lab, v in self.counters if n == name and all(lab.get(k) == x for k, x in labels.items()))


_CFG = MetricsConfig()


def _metrics(roles: tuple[str, ...] = ("publisher", "consumer")) -> tuple[BrokerMetrics, RecordingCollector]:
    collector = RecordingCollector()
    return BrokerMetrics(collector, _CFG, roles=roles), collector


def _message(**headers: Any) -> RabbitMessage:
    headers = {"x-rabbitkit-original-queue": "orders", **headers}
    return RabbitMessage(body=b"{}", routing_key="orders.123", headers=headers)


# ── reason codes ──────────────────────────────────────────────────────────


class TestConnectionFailureReason:
    @pytest.mark.parametrize(
        ("error", "reason"),
        [
            (None, "connection_lost"),
            (ConnectionRefusedError(61, "refused"), "connection_refused"),
            (ConnectionResetError("reset"), "connection_reset"),
            (BrokenPipeError(), "connection_reset"),
            (TimeoutError(), "timeout"),
            (socket.gaierror(8, "nodename nor servname provided"), "dns_failure"),
            (ssl.SSLError("bad"), "tls_error"),
            (EOFError(), "connection_lost"),
            (ValueError("x"), "other"),
            (AuthenticationFailedError("no", reason="authentication_failed"), "authentication_failed"),
            (ReconnectBudgetExhaustedError("spent", reason="reconnect_budget_exhausted"), "reconnect_budget_exhausted"),
            (TerminalConnectionError("odd", reason="made_up_reason"), "other"),
        ],
    )
    def test_maps_to_a_bounded_code(self, error: BaseException | None, reason: str) -> None:
        assert connection_failure_reason(error) == reason
        assert reason in CONNECTION_FAILURE_REASONS

    def test_client_library_errors_are_matched_by_class_name(self) -> None:
        class ConnectionClosed(Exception):  # aiormq's name
            pass

        class AMQPConnectionError(Exception):  # aiormq/pika name
            pass

        assert connection_failure_reason(ConnectionClosed(320, "CONNECTION_FORCED")) == "broker_closed"
        assert connection_failure_reason(AMQPConnectionError("gone")) == "connection_lost"

    def test_walks_the_cause_chain(self) -> None:
        class AMQPConnectionError(Exception):
            pass

        wrapped = AMQPConnectionError("wrapped")
        wrapped.__cause__ = ConnectionRefusedError(61, "refused")
        assert connection_failure_reason(wrapped) == "connection_refused"

    def test_never_returns_exception_text(self) -> None:
        assert connection_failure_reason(RuntimeError("amqp://user:secret@host")) == "other"


class TestPublishCounterLabels:
    def test_reason_label_is_bounded(self) -> None:
        collector = RecordingCollector()
        mw = MetricsMiddleware(collector)
        env = MessageEnvelope(routing_key="rk", body=b"x", exchange="orders")
        for outcome in (
            PublishOutcome(status=PublishStatus.CONFIRMED),
            PublishOutcome(status=PublishStatus.NOT_SENT, reason=PublishReason.ADMISSION_TIMEOUT),
            PublishOutcome(status=PublishStatus.UNKNOWN, reason=PublishReason.CONNECTION_LOST),
            PublishOutcome(status=PublishStatus.ERROR, reason="free text from a custom transport"),
        ):
            mw.publish_scope(lambda _e, o=outcome: o, env)
        labels = [lab for name, lab, _ in collector.counters if name == _CFG.published_total]
        assert [(lab["status"], lab["reason"]) for lab in labels] == [
            ("confirmed", "none"),
            ("not_sent", "admission_timeout"),
            ("unknown", "connection_lost"),
            ("error", "other"),
        ]

    async def test_confirm_latency_is_left_to_the_transport_once_it_reports_it(self) -> None:
        collector = RecordingCollector()
        mw = MetricsMiddleware(collector)
        metrics, _ = _metrics()
        metrics.take_over_confirm_latency([object(), mw])
        env = MessageEnvelope(routing_key="rk", body=b"x")

        async def confirmed(_e: MessageEnvelope) -> PublishOutcome:
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        await mw.publish_scope_async(confirmed, env)
        assert [n for n, _, _ in collector.histograms] == [_CFG.publish_seconds]


class TestConsumeFailureCategory:
    async def test_failures_carry_the_error_category(self) -> None:
        from rabbitkit.core.errors import InvalidMessageError

        collector = RecordingCollector()
        mw = MetricsMiddleware(collector)

        async def bad(_m: RabbitMessage) -> None:
            raise InvalidMessageError("schema")

        with pytest.raises(InvalidMessageError):
            await mw.consume_scope_async(bad, _message())
        assert collector.count(_CFG.messages_failed_total, queue="orders", category="invalid_message") == 1

        def timeout(_m: RabbitMessage) -> None:
            raise TimeoutError

        with pytest.raises(TimeoutError):
            mw.consume_scope(timeout, _message())
        assert collector.count(_CFG.messages_failed_total, queue="orders", category="retryable") == 1


# ── BrokerMetrics ─────────────────────────────────────────────────────────


class TestBrokerMetricsConnections:
    def test_broker_connected_follows_every_role(self) -> None:
        metrics, c = _metrics()
        metrics.connection_state("publisher", True)
        assert c.gauge(_CFG.broker_connected) == 0.0  # consumer not up yet
        metrics.connection_state("consumer", True)
        assert c.gauge(_CFG.broker_connected) == 1.0
        assert c.gauge(_CFG.connections_active) == 2.0
        metrics.connection_state("consumer", False, ConnectionResetError())
        assert c.gauge(_CFG.broker_connected) == 0.0
        assert c.gauge(_CFG.connection_up, role="consumer") == 0.0
        assert c.gauge(_CFG.connection_up, role="publisher") == 1.0
        assert c.count(_CFG.connection_failures_total, role="consumer", reason="connection_reset") == 1
        metrics.connection_state("consumer", True)
        assert c.gauge(_CFG.broker_connected) == 1.0

    def test_unknown_roles_are_folded_into_a_bounded_value(self) -> None:
        metrics, c = _metrics()
        metrics.connection_state("tenant-42", False)
        metrics.reconnect_failed("tenant-42", None)
        roles = {lab["role"] for _k, _n, lab in c.emitted if "role" in lab}
        assert roles <= {"publisher", "consumer", "connection"}
        assert "connection" in roles

    def test_failed_reconnect_attempts(self) -> None:
        metrics, c = _metrics()
        metrics.reconnect_failed("publisher", ConnectionRefusedError())
        assert c.count(_CFG.reconnect_attempts_total, role="publisher") == 1
        assert c.count(_CFG.connection_failures_total, role="publisher", reason="connection_refused") == 1

    def test_terminal_failure_counts_its_reason_and_marks_the_role_down(self) -> None:
        metrics, c = _metrics()
        metrics.set_states({"publisher": True, "consumer": True, "ignored": True})
        error = AuthenticationFailedError("no", reason="authentication_failed", role="consumer")
        metrics.connection_failed(error)
        assert c.count(_CFG.connection_failures_total, role="consumer", reason="authentication_failed") == 1
        assert c.gauge(_CFG.broker_connected) == 0.0
        metrics.connection_failed(ReconnectBudgetExhaustedError("x", reason="reconnect_budget_exhausted", role="odd"))
        assert c.count(_CFG.connection_failures_total, role="connection", reason="reconnect_budget_exhausted") == 1

    def test_stopped_zeroes_the_sampled_gauges(self) -> None:
        metrics, c = _metrics()
        metrics.set_states({"publisher": True, "consumer": True})
        metrics.sample(channels_active=5, publish_in_flight=3, connection_blocked=True, consuming={"orders": True})
        metrics.stopped(["orders"])
        assert c.gauge(_CFG.broker_connected) == 0.0
        assert c.gauge(_CFG.channels_active) == 0.0
        assert c.gauge(_CFG.publish_in_flight) == 0.0
        assert c.gauge(_CFG.connection_blocked) == 0.0
        assert c.gauge(_CFG.route_consuming, queue="orders") == 0.0

    def test_counters_for_churn(self) -> None:
        metrics, c = _metrics()
        metrics.reconnected()
        metrics.channel_opened()
        metrics.channel_rebuilt()
        assert [n for n, _, _ in c.counters] == [
            _CFG.reconnects_total,
            _CFG.channels_opened_total,
            _CFG.channel_rebuilds_total,
        ]

    def test_a_collector_without_set_gauge_is_tolerated(self) -> None:
        class CountersOnly:
            def __init__(self) -> None:
                self.names: list[str] = []

            def inc_counter(self, name: str, labels: dict[str, str], value: float = 1.0) -> None:
                self.names.append(name)

            def observe_histogram(self, name: str, labels: dict[str, str], value: float) -> None:
                self.names.append(name)

        collector = CountersOnly()
        metrics = BrokerMetrics(collector, _CFG, roles=("shared",))  # type: ignore[arg-type]  # protocol minus set_gauge
        metrics.connection_state("shared", False)
        metrics.blocked(True)
        assert collector.names == [_CFG.connection_failures_total]
        assert metrics.config is _CFG


class TestBrokerMetricsConsumersAndPublishing:
    def test_broker_cancelled_consumer(self) -> None:
        metrics, c = _metrics()
        metrics.consumer_cancelled("orders")
        assert c.count(_CFG.consumer_cancelled_total, queue="orders") == 1
        assert c.gauge(_CFG.route_consuming, queue="orders") == 0.0
        metrics.consumer_resumed("orders")
        assert c.gauge(_CFG.route_consuming, queue="orders") == 1.0

    def test_confirm_latency_only_for_confirmed_answers(self) -> None:
        metrics, c = _metrics()
        metrics.publish_outcome(PublishOutcome(status=PublishStatus.CONFIRMED, exchange="orders"), 0.004)
        metrics.publish_outcome(PublishOutcome(status=PublishStatus.CONFIRMED), None)
        metrics.publish_outcome(PublishOutcome(status=PublishStatus.NACKED, exchange="orders"), 0.002)
        assert c.histograms == [(_CFG.publish_confirm_latency_seconds, {"exchange": "orders"}, 0.004)]

    def test_routing_failures(self) -> None:
        metrics, c = _metrics()

        class ChannelClosedByBroker(Exception):
            reply_code = 404

        metrics.publish_outcome(PublishOutcome(status=PublishStatus.RETURNED, exchange="orders"), 0.001)
        metrics.publish_outcome(
            PublishOutcome(status=PublishStatus.ERROR, exchange="gone", reason=PublishReason.EXCHANGE_NOT_FOUND), None
        )
        metrics.publish_outcome(PublishOutcome(status=PublishStatus.ERROR, error=ChannelClosedByBroker()), None)
        metrics.publish_outcome(PublishOutcome(status=PublishStatus.ERROR, reason=PublishReason.ACCESS_REFUSED), None)
        metrics.publish_outcome(PublishOutcome(status=PublishStatus.ERROR, error=ValueError()), None)
        assert [lab for n, lab, _ in c.counters if n == _CFG.publish_routing_failures_total] == [
            {"exchange": "orders", "reason": "returned"},
            {"exchange": "gone", "reason": "exchange_not_found"},
            {"exchange": "default", "reason": "exchange_not_found"},
        ]

    def test_sample_skips_unavailable_values(self) -> None:
        metrics, c = _metrics()
        metrics.sample(publish_in_flight=2)
        metrics.sample_worker_pool(7)
        assert c.gauge(_CFG.publish_in_flight) == 2.0
        assert c.gauge(_CFG.publish_in_flight_bytes) is None
        assert c.gauge(_CFG.worker_pool_pending) == 7.0


# ── inventory and bounded labels ──────────────────────────────────────────


def _emit_everything(collector: RecordingCollector) -> None:
    """Drive every emission path a unit test can reach without a broker."""
    mw = MetricsMiddleware(collector)
    env = MessageEnvelope(routing_key="orders.123", body=b"x", exchange="orders")
    for status in PublishStatus:
        for reason in [None, *sorted(PublishReason.ALL)]:
            mw.publish_scope(lambda _e, s=status, r=reason: PublishOutcome(status=s, reason=r), env)
    mw.publish_scope(lambda _e: "custom", env)
    with pytest.raises(RuntimeError):
        mw.publish_scope(lambda _e: (_ for _ in ()).throw(RuntimeError("x")), env)
    mw.consume_scope(lambda _m: None, _message())
    redelivered = RabbitMessage(
        body=b"", routing_key="orders.123", redelivered=True, headers={"x-rabbitkit-original-queue": "orders"}
    )
    mw.consume_scope(lambda _m: None, redelivered)
    with pytest.raises(ValueError):
        mw.consume_scope(lambda _m: (_ for _ in ()).throw(ValueError("x")), _message())
    for disposition in ("acked", "nacked", "rejected"):
        mw.record_settlement(_message(), disposition)
    mw.record_settlement_outcome_unknown(_message())

    metrics = BrokerMetrics(collector, _CFG, roles=("publisher", "consumer"))
    for role in ("publisher", "consumer", "shared", "connection", "unbounded-role"):
        metrics.connection_state(role, True)
        for reason_error in (None, ConnectionRefusedError(), TimeoutError(), ValueError("x")):
            metrics.connection_state(role, False, reason_error)
            metrics.reconnect_failed(role, reason_error)
    for reason in sorted(CONNECTION_FAILURE_REASONS):
        metrics.connection_failed(TerminalConnectionError("x", reason=reason, role="publisher"))
    metrics.reconnected()
    metrics.channel_opened()
    metrics.channel_rebuilt()
    metrics.blocked(True)
    metrics.consumer_cancelled("orders")
    metrics.consumer_resumed("orders")
    for status in PublishStatus:
        metrics.publish_outcome(PublishOutcome(status=status, exchange="orders"), 0.001)
    metrics.publish_outcome(PublishOutcome(status=PublishStatus.ERROR, reason=PublishReason.EXCHANGE_NOT_FOUND), None)
    metrics.sample(
        channels_active=1,
        publish_in_flight=1,
        publish_in_flight_bytes=1,
        publish_channels_in_use=1,
        connection_blocked=False,
        consuming={"orders": True},
    )
    metrics.sample_worker_pool(0)
    metrics.stopped(["orders"])


class TestBoundedLabels:
    def test_every_inventory_label_is_in_the_allow_list(self) -> None:
        allowed = bounded_label_values()
        for spec in metric_inventory():
            assert set(spec.labels) <= set(allowed), spec
            assert spec.kind in {"counter", "gauge", "histogram"}
        names = [spec.name for spec in metric_inventory()]
        assert len(names) == len(set(names))

    def test_every_emission_matches_the_inventory_and_bounded_values(self) -> None:
        collector = RecordingCollector()
        _emit_everything(collector)
        specs = {spec.name: spec for spec in metric_inventory()}
        allowed = bounded_label_values()
        assert collector.emitted
        for kind, name, labels in collector.emitted:
            spec = specs.get(name)
            assert spec is not None, f"{name} is emitted but not in metric_inventory()"
            assert spec.kind == kind, name
            assert tuple(sorted(labels)) == tuple(sorted(spec.labels)), (name, labels)
            for label, value in labels.items():
                values = allowed[label]
                if values is not None:
                    assert value in values, (name, label, value)
        # a routing key with an id never becomes a label value
        assert all("orders.123" not in labels.values() for _k, _n, labels in collector.emitted)

    def test_category_values_are_the_error_taxonomy(self) -> None:
        assert bounded_label_values()["category"] == frozenset(c.value for c in ErrorCategory)

    def test_inventory_follows_the_namespace(self) -> None:
        names = {spec.name for spec in metric_inventory(MetricsConfig(namespace="svc"))}
        assert "svc_connection_up" in names
        assert "rabbitkit_health_state" in names  # HealthWatcher's own default


class TestDocsInventory:
    def test_docs_table_lists_exactly_the_inventory(self) -> None:
        """docs/observability.md is generated from metric_inventory(); keep them in sync."""
        text = _DOCS.read_text()
        start = text.index("<!-- metric-inventory:start -->")
        end = text.index("<!-- metric-inventory:end -->")
        rows = [line for line in text[start:end].splitlines() if line.startswith("| `")]
        documented = {}
        for row in rows:
            cells = [cell.strip() for cell in row.strip("|").split("|")]
            documented[cells[0].strip("`")] = (cells[1], cells[2])
        expected = {
            spec.name: (spec.kind, ", ".join(f"`{label}`" for label in spec.labels) or "—")
            for spec in metric_inventory()
        }
        assert documented == expected
