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
def collector(registry: object, monkeypatch: pytest.MonkeyPatch) -> PrometheusCollector:
    coll = PrometheusCollector()
    original = coll._prometheus_client

    class Scoped:
        def Counter(self, *a: object, **k: object) -> object:  # noqa: N802 — mirrors prometheus_client
            return original.Counter(*a, registry=registry, **k)

        def Histogram(self, *a: object, **k: object) -> object:  # noqa: N802
            return original.Histogram(*a, registry=registry, **k)

        def Gauge(self, *a: object, **k: object) -> object:  # noqa: N802
            return original.Gauge(*a, registry=registry, **k)

    coll._prometheus_client = Scoped()
    return coll


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
