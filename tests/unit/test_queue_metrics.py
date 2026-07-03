"""Tests for queue_metrics.py — QueueMetricsPoller (H5)."""

from __future__ import annotations

from typing import Any

import pytest

from rabbitkit.core.config import MetricsConfig
from rabbitkit.queue_metrics import QueueMetricsPoller

# ── fakes ───────────────────────────────────────────────────────────────


class _RecordingCollector:
    """Captures set_gauge calls as (name, labels, value) tuples."""

    def __init__(self) -> None:
        self.gauges: list[tuple[str, dict[str, str], float]] = []

    def set_gauge(self, name: str, labels: dict[str, str], value: float) -> None:
        self.gauges.append((name, dict(labels), value))

    def inc_counter(self, name: str, labels: dict[str, str], value: float = 1.0) -> None: ...
    def observe_histogram(self, name: str, labels: dict[str, str], value: float) -> None: ...


class _FakeMgmt:
    def __init__(self, queues: list[dict[str, Any]] | Exception) -> None:
        self._queues = queues

    def list_queues(self, vhost: str = "/") -> list[dict[str, Any]]:
        if isinstance(self._queues, Exception):
            raise self._queues
        return self._queues

    async def list_queues_async(self, vhost: str = "/") -> list[dict[str, Any]]:
        if isinstance(self._queues, Exception):
            raise self._queues
        return self._queues


_SAMPLE = [
    {
        "name": "orders",
        "messages": 150,
        "messages_ready": 100,
        "messages_unacknowledged": 50,
        "consumers": 3,
    },
    {
        "name": "orders.dlq",
        "messages": 7,
        "messages_ready": 7,
        "messages_unacknowledged": 0,
        "consumers": 0,
    },
]


def _by_queue(collector: _RecordingCollector, queue: str) -> dict[str, float]:
    cfg = MetricsConfig()
    keymap = {
        cfg.queue_messages_ready: "ready",
        cfg.queue_messages_unacked: "unacked",
        cfg.queue_messages_total: "total",
        cfg.queue_consumers: "consumers",
    }
    return {
        keymap[name]: value
        for (name, labels, value) in collector.gauges
        if labels.get("queue") == queue and name in keymap
    }


# ── poll_once ───────────────────────────────────────────────────────────


class TestPollOnce:
    def test_emits_all_gauges_per_queue(self) -> None:
        collector = _RecordingCollector()
        poller = QueueMetricsPoller(_FakeMgmt(_SAMPLE), collector)

        count = poller.poll_once()

        assert count == 2
        orders = _by_queue(collector, "orders")
        assert orders == {"ready": 100, "unacked": 50, "total": 150, "consumers": 3}
        dlq = _by_queue(collector, "orders.dlq")
        assert dlq == {"ready": 7, "unacked": 0, "total": 7, "consumers": 0}

    def test_dlq_zero_consumers_visible(self) -> None:
        """A DLQ with 0 consumers is exactly the signal operators must alert on."""
        collector = _RecordingCollector()
        QueueMetricsPoller(_FakeMgmt(_SAMPLE), collector).poll_once()
        assert _by_queue(collector, "orders.dlq")["consumers"] == 0

    def test_missing_counts_default_to_zero(self) -> None:
        collector = _RecordingCollector()
        poller = QueueMetricsPoller(_FakeMgmt([{"name": "q"}]), collector)
        poller.poll_once()
        assert _by_queue(collector, "q") == {"ready": 0, "unacked": 0, "total": 0, "consumers": 0}

    def test_total_derived_when_absent(self) -> None:
        collector = _RecordingCollector()
        poller = QueueMetricsPoller(
            _FakeMgmt([{"name": "q", "messages_ready": 4, "messages_unacknowledged": 6}]),
            collector,
        )
        poller.poll_once()
        assert _by_queue(collector, "q")["total"] == 10

    def test_unnamed_queue_skipped(self) -> None:
        collector = _RecordingCollector()
        poller = QueueMetricsPoller(_FakeMgmt([{"messages_ready": 5}]), collector)
        assert poller.poll_once() == 0
        assert collector.gauges == []

    def test_queue_filter_applied(self) -> None:
        collector = _RecordingCollector()
        poller = QueueMetricsPoller(
            _FakeMgmt(_SAMPLE),
            collector,
            queue_filter=lambda name: not name.endswith(".dlq"),
        )
        count = poller.poll_once()
        assert count == 1
        assert _by_queue(collector, "orders")  # emitted
        assert _by_queue(collector, "orders.dlq") == {}  # filtered out

    def test_management_error_is_swallowed(self) -> None:
        """A management-API outage must not crash the poller — skip and retry."""
        collector = _RecordingCollector()
        poller = QueueMetricsPoller(_FakeMgmt(ConnectionError("mgmt down")), collector)
        assert poller.poll_once() == 0
        assert collector.gauges == []

    def test_custom_namespace(self) -> None:
        collector = _RecordingCollector()
        poller = QueueMetricsPoller(
            _FakeMgmt([{"name": "q", "messages_ready": 1}]),
            collector,
            config=MetricsConfig(namespace="myapp"),
        )
        poller.poll_once()
        names = {name for (name, _, _) in collector.gauges}
        assert "myapp_queue_messages_ready" in names


class TestPollOnceAsync:
    async def test_emits_gauges(self) -> None:
        collector = _RecordingCollector()
        poller = QueueMetricsPoller(_FakeMgmt(_SAMPLE), collector)
        count = await poller.poll_once_async()
        assert count == 2
        assert _by_queue(collector, "orders")["ready"] == 100

    async def test_management_error_swallowed(self) -> None:
        collector = _RecordingCollector()
        poller = QueueMetricsPoller(_FakeMgmt(ConnectionError("down")), collector)
        assert await poller.poll_once_async() == 0

    async def test_run_async_loop_polls_then_cancels(self) -> None:
        import asyncio

        collector = _RecordingCollector()
        # Tiny interval so the loop iterates quickly; cancel to stop.
        poller = QueueMetricsPoller(_FakeMgmt(_SAMPLE), collector, interval=0.01)
        task = asyncio.create_task(poller.run_async())
        # Wait until at least one poll has emitted gauges.
        for _ in range(200):
            if collector.gauges:
                break
            await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert collector.gauges


class TestBackgroundThread:
    def test_start_polls_then_stop(self) -> None:
        collector = _RecordingCollector()
        # Large interval so exactly one immediate poll runs before stop().
        poller = QueueMetricsPoller(_FakeMgmt(_SAMPLE), collector, interval=3600.0)
        poller.start()
        # Give the daemon thread a moment to run its first poll.
        import time

        deadline = time.monotonic() + 2.0
        while not collector.gauges and time.monotonic() < deadline:
            time.sleep(0.01)
        poller.stop(timeout=2.0)
        assert collector.gauges  # first poll ran

    def test_double_start_is_idempotent(self) -> None:
        collector = _RecordingCollector()
        poller = QueueMetricsPoller(_FakeMgmt([]), collector, interval=3600.0)
        poller.start()
        poller.start()  # must not spawn a second thread or raise
        poller.stop()

    def test_stop_without_start_is_safe(self) -> None:
        poller = QueueMetricsPoller(_FakeMgmt([]), _RecordingCollector())
        poller.stop()  # no thread — must not raise


class TestPrometheusGauge:
    def test_set_gauge_creates_and_sets(self) -> None:
        """PrometheusCollector.set_gauge creates a Gauge and calls .set().

        Mocks the prometheus_client module (like the other PrometheusCollector
        tests) to avoid touching the process-global default registry, which
        collides across test instances under random ordering.
        """
        pytest.importorskip("prometheus_client")
        from unittest.mock import MagicMock

        from rabbitkit.middleware.metrics import PrometheusCollector

        collector = PrometheusCollector()
        mock_pc = MagicMock()
        collector._prometheus_client = mock_pc

        collector.set_gauge("rabbitkit_queue_messages_ready", {"queue": "orders"}, 42.0)

        mock_pc.Gauge.assert_called_once()
        mock_pc.Gauge.return_value.labels.assert_called_once_with(queue="orders")
        mock_pc.Gauge.return_value.labels.return_value.set.assert_called_once_with(42.0)

    def test_set_gauge_caches_metric(self) -> None:
        pytest.importorskip("prometheus_client")
        from unittest.mock import MagicMock

        from rabbitkit.middleware.metrics import PrometheusCollector

        collector = PrometheusCollector()
        mock_pc = MagicMock()
        collector._prometheus_client = mock_pc

        collector.set_gauge("cached_gauge", {"queue": "a"}, 1.0)
        collector.set_gauge("cached_gauge", {"queue": "a"}, 2.0)

        # Gauge constructor called only once despite two set_gauge calls.
        assert mock_pc.Gauge.call_count == 1
