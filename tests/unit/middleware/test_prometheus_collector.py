"""PrometheusCollector against the REAL prometheus_client, not a mock.

Every other metrics test uses a ``MagicMock`` collector, which happily accepts
anything. That hid a crash in every UNLABELLED metric rabbitkit emits:
``prometheus_client`` raises ``ValueError: No label names were set`` when you
call ``.labels()`` on a metric built without label names, so
``reconnects_total``, ``channels_opened_total``, ``channel_rebuilds_total``,
the broker/consumer lifecycle gauges, the settlement gauges and the health
gauge all raised the first time they fired — taking the caller down with them
(``broker.start()`` raised on the very first lifecycle gauge).
"""

from __future__ import annotations

import pytest

from rabbitkit.core.config import MetricsConfig

prometheus_client = pytest.importorskip("prometheus_client")

from rabbitkit.middleware.metrics import PrometheusCollector  # noqa: E402


@pytest.fixture
def registry() -> object:
    """A private registry so each test is isolated from the global default."""
    return prometheus_client.CollectorRegistry()


@pytest.fixture
def collector(registry: object) -> PrometheusCollector:
    return PrometheusCollector(registry=registry)


def _dump(registry: object) -> str:
    return prometheus_client.generate_latest(registry).decode()


class TestUnlabelledMetrics:
    """The regression: these used to raise ValueError on the first call."""

    def test_counter_without_labels(self, collector: PrometheusCollector, registry: object) -> None:
        collector.inc_counter("rk_test_reconnects_total", {})
        collector.inc_counter("rk_test_reconnects_total", {})
        assert "rk_test_reconnects_total 2.0" in _dump(registry)

    def test_counter_without_labels_with_an_explicit_value(
        self, collector: PrometheusCollector, registry: object
    ) -> None:
        collector.inc_counter("rk_test_coalesced_total", {}, 20.0)
        assert "rk_test_coalesced_total 20.0" in _dump(registry)

    def test_gauge_without_labels(self, collector: PrometheusCollector, registry: object) -> None:
        collector.set_gauge("rk_test_broker_connected", {}, 1.0)
        assert "rk_test_broker_connected 1.0" in _dump(registry)
        collector.set_gauge("rk_test_broker_connected", {}, 0.0)
        assert "rk_test_broker_connected 0.0" in _dump(registry)

    def test_histogram_without_labels(self, collector: PrometheusCollector, registry: object) -> None:
        collector.observe_histogram("rk_test_batch_size", {}, 42.0)
        dump = _dump(registry)
        assert "rk_test_batch_size_count 1.0" in dump
        assert "rk_test_batch_size_sum 42.0" in dump


class TestLabelledMetrics:
    def test_counter_with_labels(self, collector: PrometheusCollector, registry: object) -> None:
        collector.inc_counter("rk_test_consumed_total", {"queue": "orders", "status": "success"}, 3)
        dump = _dump(registry)
        assert 'queue="orders"' in dump and 'status="success"' in dump
        assert "3.0" in dump

    def test_gauge_with_labels(self, collector: PrometheusCollector, registry: object) -> None:
        collector.set_gauge("rk_test_in_flight", {"queue": "orders"}, 5.0)
        assert 'rk_test_in_flight{queue="orders"} 5.0' in _dump(registry)

    def test_histogram_with_labels(self, collector: PrometheusCollector, registry: object) -> None:
        collector.observe_histogram("rk_test_publish_seconds", {"exchange": "events"}, 0.25)
        dump = _dump(registry)
        assert 'exchange="events"' in dump
        assert "rk_test_publish_seconds_sum" in dump

    def test_label_values_are_kept_separate(self, collector: PrometheusCollector, registry: object) -> None:
        collector.inc_counter("rk_test_split_total", {"queue": "a"}, 1)
        collector.inc_counter("rk_test_split_total", {"queue": "b"}, 4)
        dump = _dump(registry)
        assert 'rk_test_split_total{queue="a"} 1.0' in dump
        assert 'rk_test_split_total{queue="b"} 4.0' in dump


class TestEveryUnlabelledMetricRabbitkitEmits:
    """Each of these is emitted with `{}` labels somewhere in the codebase."""

    @pytest.mark.parametrize(
        "attr",
        [
            "reconnects_total",
            "channels_opened_total",
            "channel_rebuilds_total",
            "broker_connected",
            "consumer_active",
            "worker_pool_pending",
            "settlement_pending",
            "settlement_ack_ready",
            "settlement_frontier",
            "settlement_gap_count",
            "settlement_oldest_pending_age_seconds",
            "settlement_coalescing_ratio",
            "settlement_coalesced_total",
            "bulk_publish_batch_size",
        ],
    )
    def test_emitting_it_unlabelled_does_not_raise(
        self, collector: PrometheusCollector, registry: object, attr: str
    ) -> None:
        name = getattr(MetricsConfig(namespace="rk_probe"), attr)
        if name.endswith("_total"):
            collector.inc_counter(name, {}, 1.0)
        elif name.endswith(("_size", "_seconds")) and "age_seconds" not in name:
            collector.observe_histogram(name, {}, 1.0)
        else:
            collector.set_gauge(name, {}, 1.0)
        assert name in _dump(registry)


class TestRepeatedUse:
    def test_the_same_metric_is_reused_not_redefined(self, collector: PrometheusCollector, registry: object) -> None:
        for _ in range(5):
            collector.inc_counter("rk_test_reused_total", {})
        assert "rk_test_reused_total 5.0" in _dump(registry)

    def test_switching_from_unlabelled_to_labelled_name_is_independent(
        self, collector: PrometheusCollector, registry: object
    ) -> None:
        collector.inc_counter("rk_test_plain_total", {})
        collector.inc_counter("rk_test_tagged_total", {"queue": "q"})
        dump = _dump(registry)
        assert "rk_test_plain_total 1.0" in dump
        assert 'rk_test_tagged_total{queue="q"} 1.0' in dump


class TestInjectableRegistry:
    """the collector no longer forces the global registry, and a second
    collector on the same registry reuses metrics instead of raising."""

    def test_default_is_the_global_registry(self) -> None:
        assert PrometheusCollector().registry is prometheus_client.REGISTRY

    def test_metrics_go_to_the_injected_registry_only(self, registry: object) -> None:
        collector = PrometheusCollector(registry=registry)
        collector.inc_counter("rk_test_injected_total", {"queue": "orders"})
        assert collector.registry is registry
        assert 'rk_test_injected_total{queue="orders"} 1.0' in _dump(registry)
        assert "rk_test_injected_total" not in prometheus_client.generate_latest().decode()

    def test_two_collectors_share_one_registry(self, registry: object) -> None:
        first = PrometheusCollector(registry=registry)
        second = PrometheusCollector(registry=registry)
        first.inc_counter("rk_test_shared_total", {"queue": "a"})
        second.inc_counter("rk_test_shared_total", {"queue": "a"})
        second.observe_histogram("rk_test_shared_seconds", {}, 1.0)
        first.observe_histogram("rk_test_shared_seconds", {}, 1.0)
        first.set_gauge("rk_test_shared_gauge", {}, 1.0)
        second.set_gauge("rk_test_shared_gauge", {}, 3.0)
        dump = _dump(registry)
        assert 'rk_test_shared_total{queue="a"} 2.0' in dump
        assert "rk_test_shared_seconds_count 2.0" in dump
        assert "rk_test_shared_gauge 3.0" in dump

    def test_a_conflicting_registration_still_raises(self, registry: object) -> None:
        PrometheusCollector(registry=registry).inc_counter("rk_test_conflict_total", {"queue": "a"})
        with pytest.raises(ValueError, match="Duplicated"):
            PrometheusCollector(registry=registry).inc_counter("rk_test_conflict_total", {"exchange": "a"})
        with pytest.raises(ValueError, match="Duplicated"):
            PrometheusCollector(registry=registry).set_gauge("rk_test_conflict_total", {"queue": "a"}, 1.0)
