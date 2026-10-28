"""Real-broker coverage for the observability foundation (G-22, G-23, PUB-017, CON-005).

* publish outcomes are counted by status AND bounded reason; routing failures
  (returned / exchange_not_found) are counted by the broker itself; confirm
  latency is the send-to-confirm time (PrometheusCollector, private registry)
* a publish-only broker gets connection and channel gauges
* ``connection_up`` / ``broker_connected`` drop when the connections are
  killed and come back after the reconnect; the failure reason is counted
* failed reconnect attempts are counted per role while the broker is down
* ``channels_active`` stays bounded under concurrent publishing
* ``connection_blocked`` follows a disk alarm (connection.blocked/unblocked)
* a trace id propagates producer -> RabbitMQ -> consumer, and a retry copy
  stays in the producer's trace
* the shipped alert rules pass ``promtool check rules`` / ``promtool test rules``

The broker is this module's own: tests kill every connection on it, stop its
AMQP listener and raise a node-wide disk alarm.
"""

from __future__ import annotations

import asyncio
import contextlib
import shutil
import subprocess
import threading
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.integration.conftest import await_consumers, skip_without_docker

pytestmark = pytest.mark.integration

_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def own_rabbit() -> Iterator[dict[str, Any]]:
    skip_without_docker()
    from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

    with RabbitMqContainer("rabbitmq:3.13-management-alpine") as container:
        host = container.get_container_host_ip()
        yield {
            "url": f"amqp://guest:guest@{host}:{container.get_exposed_port(5672)}/",
            "container": container,
        }


def _ctl(rabbit: dict[str, Any], *args: str) -> str:
    result = rabbit["container"].get_wrapped_container().exec_run(["rabbitmqctl", *args])
    output = result[1] if isinstance(result, tuple) else result.output
    return output.decode("utf-8", "replace") if isinstance(output, bytes) else str(output)


async def actl(rabbit: dict[str, Any], *args: str) -> str:
    return await asyncio.to_thread(_ctl, rabbit, *args)


def _name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _config(rabbit: dict[str, Any], **kw: Any) -> Any:
    import dataclasses

    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    conn = dataclasses.replace(
        ConnectionConfig.from_url(rabbit["url"]), reconnect_backoff_base=0.2, reconnect_backoff_max=1.0
    )
    return RabbitConfig(connection=conn, **kw)


def _declare(rabbit: dict[str, Any], queue: str) -> None:
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(rabbit["url"]))
    try:
        conn.channel().queue_declare(queue)
    finally:
        conn.close()


class History:
    """MetricsCollector that keeps every value it was given (thread-safe)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.counters: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}
        self.gauges: dict[tuple[str, tuple[tuple[str, str], ...]], list[float]] = {}
        self.histograms: dict[tuple[str, tuple[tuple[str, str], ...]], list[float]] = {}

    @staticmethod
    def _key(name: str, labels: dict[str, str]) -> tuple[str, tuple[tuple[str, str], ...]]:
        return name, tuple(sorted(labels.items()))

    def inc_counter(self, name: str, labels: dict[str, str], value: float = 1.0) -> None:
        with self._lock:
            key = self._key(name, labels)
            self.counters[key] = self.counters.get(key, 0.0) + value

    def observe_histogram(self, name: str, labels: dict[str, str], value: float) -> None:
        with self._lock:
            self.histograms.setdefault(self._key(name, labels), []).append(value)

    def set_gauge(self, name: str, labels: dict[str, str], value: float) -> None:
        with self._lock:
            self.gauges.setdefault(self._key(name, labels), []).append(value)

    def count(self, name: str, **labels: str) -> float:
        with self._lock:
            return sum(
                v
                for (n, lab), v in self.counters.items()
                if n == name and all(dict(lab).get(k) == x for k, x in labels.items())
            )

    def gauge_history(self, name: str, **labels: str) -> list[float]:
        with self._lock:
            return list(self.gauges.get(self._key(name, labels), []))

    def gauge(self, name: str, **labels: str) -> float | None:
        values = self.gauge_history(name, **labels)
        return values[-1] if values else None


async def _until(predicate: Any, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        await asyncio.sleep(0.1)


@contextlib.asynccontextmanager
async def _started(broker: Any) -> AsyncIterator[Any]:
    await broker.start(install_signal_handlers=False)
    try:
        yield broker
    finally:
        async with asyncio.timeout(30):
            await broker.stop()


def _scrape(registry: Any) -> str:
    import prometheus_client

    return prometheus_client.generate_latest(registry).decode()


# ── publish outcomes, routing failures, confirm latency ──────────────────


async def test_publish_outcomes_routing_failures_and_confirm_latency(own_rabbit: dict[str, Any]) -> None:
    """Before: messages_published_total had no reason, there was no routing
    failure counter, a publish-only broker had no connection metrics, and
    confirm latency timed the whole call."""
    prometheus_client = pytest.importorskip("prometheus_client")
    from rabbitkit import AsyncBroker
    from rabbitkit.core.config import PublisherConfig
    from rabbitkit.core.types import MessageEnvelope, PublishStatus
    from rabbitkit.middleware.metrics import MetricsMiddleware, PrometheusCollector

    registry = prometheus_client.CollectorRegistry()
    queue = _name("rk-obs-pub")
    await asyncio.to_thread(_declare, own_rabbit, queue)
    broker = AsyncBroker(
        _config(own_rabbit, publisher=PublisherConfig(max_message_bytes=0, max_in_flight_bytes=64)),
        middlewares=[MetricsMiddleware(PrometheusCollector(registry=registry))],
    )
    async with asyncio.timeout(60), _started(broker):
        confirmed = await broker.publish(MessageEnvelope(routing_key=queue, body=b"x"))
        returned = await broker.publish(MessageEnvelope(routing_key=_name("nowhere"), body=b"x", mandatory=True))
        missing = await broker.publish(MessageEnvelope(exchange=_name("no-such-exchange"), routing_key="k", body=b"x"))
        not_sent = await broker.publish(MessageEnvelope(routing_key=queue, body=b"y" * 100))
        assert [o.status for o in (confirmed, returned, missing, not_sent)] == [
            PublishStatus.CONFIRMED,
            PublishStatus.RETURNED,
            PublishStatus.ERROR,
            PublishStatus.NOT_SENT,
        ]
        broker._sample_metrics()
        text = _scrape(registry)

    def has(sample: str) -> bool:
        return any(line.startswith(sample) and not line.endswith(" 0.0") for line in text.splitlines())

    assert has('rabbitkit_messages_published_total{exchange="default",reason="none",status="confirmed"}')
    assert has('rabbitkit_messages_published_total{exchange="default",reason="returned",status="returned"}')
    assert has(
        'rabbitkit_messages_published_total{exchange="default",reason="exceeds_max_in_flight_bytes",status="not_sent"}'
    )
    missing_ex = missing.exchange
    assert has(
        f'rabbitkit_messages_published_total{{exchange="{missing_ex}",reason="exchange_not_found",status="error"}}'
    )
    assert has('rabbitkit_publish_routing_failures_total{exchange="default",reason="returned"}')
    assert has(f'rabbitkit_publish_routing_failures_total{{exchange="{missing_ex}",reason="exchange_not_found"}}')
    # Transport-timed: exactly the one CONFIRMED publish, with a positive duration.
    assert 'rabbitkit_publish_confirm_latency_seconds_count{exchange="default"} 1.0' in text
    # Publish-only broker: connection and channel gauges are there.
    assert 'rabbitkit_connection_up{role="publisher"} 1.0' in text
    assert 'rabbitkit_connection_up{role="consumer"} 1.0' in text
    assert has("rabbitkit_channels_active ")
    assert has("rabbitkit_channels_opened_total ")


async def test_sync_broker_counts_routing_failures_and_connection_state(own_rabbit: dict[str, Any]) -> None:
    from rabbitkit import SyncBroker
    from rabbitkit.core.types import MessageEnvelope, PublishStatus
    from rabbitkit.middleware.metrics import MetricsMiddleware

    history = History()
    broker = SyncBroker(_config(own_rabbit), middlewares=[MetricsMiddleware(history)])

    def run() -> None:
        broker.start()
        try:
            outcome = broker.publish(MessageEnvelope(routing_key=_name("nowhere"), body=b"x", mandatory=True))
            assert outcome.status is PublishStatus.RETURNED
            assert broker.publish(MessageEnvelope(routing_key="", body=b"x")).ok
        finally:
            broker.stop()

    await asyncio.wait_for(asyncio.to_thread(run), 60)
    assert history.count("rabbitkit_publish_routing_failures_total", reason="returned") == 1
    assert history.gauge_history("rabbitkit_connection_up", role="shared")[:1] == [1.0]
    assert history.gauge("rabbitkit_connection_up", role="shared") == 0.0  # stopped
    latency = history.histograms.get(("rabbitkit_publish_confirm_latency_seconds", (("exchange", "default"),)), [])
    assert len(latency) == 1  # the CONFIRMED publish only, not the RETURNED one
    assert latency[0] > 0


# ── connection state follows drops and reconnects ────────────────────────


async def test_connection_up_drops_and_recovers_when_connections_are_killed(own_rabbit: dict[str, Any]) -> None:
    """Before: broker_connected was set only in start()/stop() and stayed 1
    through an outage; there was no per-role connection_up."""
    from rabbitkit import AsyncBroker
    from rabbitkit.middleware.metrics import MetricsMiddleware

    history = History()
    queue = _name("rk-obs-kill")
    broker = AsyncBroker(_config(own_rabbit))

    @broker.subscriber(queue=queue, middlewares=[MetricsMiddleware(history)])
    async def handle(body: bytes) -> None:
        pass

    async with asyncio.timeout(90), _started(broker):
        assert history.gauge("rabbitkit_broker_connected") == 1.0
        await actl(own_rabbit, "close_all_connections", "observability test")
        await _until(
            lambda: (
                0.0 in history.gauge_history("rabbitkit_connection_up", role="publisher")
                and 0.0 in history.gauge_history("rabbitkit_connection_up", role="consumer")
            ),
            20,
            "connection_up to drop for both roles",
        )
        assert 0.0 in history.gauge_history("rabbitkit_broker_connected")
        await _until(
            lambda: (
                history.gauge("rabbitkit_connection_up", role="publisher") == 1.0
                and history.gauge("rabbitkit_connection_up", role="consumer") == 1.0
                and history.gauge("rabbitkit_broker_connected") == 1.0
            ),
            30,
            "the reconnect to be reflected",
        )
        assert history.count("rabbitkit_connection_failures_total", reason="broker_closed") >= 2
        await _until(lambda: history.count("rabbitkit_reconnects_total") >= 2, 20, "reconnects_total")
        await await_consumers(own_rabbit["url"], queue, timeout=30)
        broker._sample_metrics()
        assert history.gauge("rabbitkit_route_consuming", queue=queue) == 1.0


async def test_failed_reconnect_attempts_are_counted_per_role(own_rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker
    from rabbitkit.middleware.metrics import MetricsMiddleware

    history = History()
    broker = AsyncBroker(_config(own_rabbit), middlewares=[MetricsMiddleware(history)])
    async with asyncio.timeout(120), _started(broker):
        await actl(own_rabbit, "stop_app")
        try:
            await _until(
                lambda: (
                    history.count("rabbitkit_reconnect_attempts_total", role="publisher") >= 2
                    and history.count("rabbitkit_reconnect_attempts_total", role="consumer") >= 2
                ),
                40,
                "two failed reconnect attempts per role",
            )
            assert history.gauge("rabbitkit_broker_connected") == 0.0
        finally:
            await actl(own_rabbit, "start_app")
            await actl(own_rabbit, "await_startup")
        await _until(lambda: history.gauge("rabbitkit_broker_connected") == 1.0, 40, "the reconnect")
    reasons = {
        dict(labels)["reason"]
        for (name, labels), value in history.counters.items()
        if name == "rabbitkit_connection_failures_total" and value
    }
    assert reasons <= {"connection_refused", "connection_reset", "connection_lost", "broker_closed", "timeout", "other"}
    assert reasons & {"connection_refused", "connection_reset", "connection_lost"}


# ── channels stay bounded ────────────────────────────────────────────────


async def test_channels_active_stays_bounded_under_concurrent_publishes(own_rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker
    from rabbitkit.core.config import PoolConfig
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.middleware.metrics import MetricsMiddleware

    history = History()
    queue = _name("rk-obs-chan")
    pool_size = 4
    broker = AsyncBroker(_config(own_rabbit, pool=PoolConfig(channel_pool_size=pool_size)))

    @broker.subscriber(queue=queue, middlewares=[MetricsMiddleware(history)])
    async def handle(body: bytes) -> None:
        pass

    bound = 2 * pool_size + 2 + 1  # publisher + mandatory pools, topology + fast, one consumer
    samples: list[float] = []
    async with asyncio.timeout(90), _started(broker):
        for _ in range(3):
            outcomes = await asyncio.gather(
                *(broker.publish(MessageEnvelope(routing_key=queue, body=b"x")) for _ in range(100)),
                *(broker.publish(MessageEnvelope(routing_key=queue, body=b"x", mandatory=True)) for _ in range(20)),
            )
            assert all(o.ok for o in outcomes)
            broker._sample_metrics()
            value = history.gauge("rabbitkit_channels_active")
            assert value is not None and 1 <= value <= bound, value
            samples.append(value)
        assert samples[-1] <= samples[0], samples  # no growth once the pools are full
        assert history.gauge("rabbitkit_publish_in_flight") == 0.0
        assert history.gauge("rabbitkit_publish_channels_in_use") == 0.0


# ── connection.blocked ───────────────────────────────────────────────────


async def test_connection_blocked_gauge_follows_a_disk_alarm(own_rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.middleware.metrics import MetricsMiddleware

    history = History()
    broker = AsyncBroker(_config(own_rabbit), middlewares=[MetricsMiddleware(history)])
    async with asyncio.timeout(90), _started(broker):
        assert history.gauge("rabbitkit_connection_blocked") == 0.0
        trigger: asyncio.Task[Any] | None = None
        try:
            await actl(own_rabbit, "set_disk_free_limit", "100000GB")
            # RabbitMQ blocks a connection when it next publishes.
            trigger = asyncio.create_task(broker.publish(MessageEnvelope(routing_key="", body=b"t"), timeout=20))
            await _until(lambda: history.gauge("rabbitkit_connection_blocked") == 1.0, 20, "connection.blocked")
        finally:
            await actl(own_rabbit, "set_disk_free_limit", "50MB")
        await _until(lambda: history.gauge("rabbitkit_connection_blocked") == 0.0, 30, "connection.unblocked")
        if trigger is not None:
            await trigger


# ── trace propagation ────────────────────────────────────────────────────


@pytest.fixture
def spans() -> Iterator[Any]:
    pytest.importorskip("opentelemetry.sdk")
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import SimpleSpanProcessor
    from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

    provider = trace.get_tracer_provider()
    if not isinstance(provider, TracerProvider):
        trace.set_tracer_provider(TracerProvider())
        provider = trace.get_tracer_provider()
    assert isinstance(provider, TracerProvider)
    exporter = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    yield exporter
    exporter.clear()
    exporter.shutdown()


async def test_trace_id_propagates_producer_to_consumer_and_through_a_retry(
    own_rabbit: dict[str, Any], spans: Any
) -> None:
    from opentelemetry import trace

    from rabbitkit import AsyncBroker, RetryConfig
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.middleware.otel import OTelTracingMiddleware

    queue = _name("rk-obs-trace")
    received: asyncio.Queue[int] = asyncio.Queue()
    attempts = 0
    producer = AsyncBroker(_config(own_rabbit), middlewares=[OTelTracingMiddleware()])
    consumer = AsyncBroker(_config(own_rabbit, retry=RetryConfig(max_retries=1, delays=(1,))))

    @consumer.subscriber(queue=queue, middlewares=[OTelTracingMiddleware()])
    async def handle(body: bytes) -> None:
        nonlocal attempts
        attempts += 1
        received.put_nowait(attempts)
        if attempts == 1:
            raise TimeoutError("transient")  # retried through the delay queue

    async with asyncio.timeout(60), _started(consumer), _started(producer):
        await await_consumers(own_rabbit["url"], queue)
        outcome = await producer.publish(MessageEnvelope(routing_key=queue, body=b"{}"))
        assert outcome.ok
        assert await received.get() == 1
        assert await received.get() == 2  # the retry copy

    finished = spans.get_finished_spans()
    sends = [s for s in finished if s.kind == trace.SpanKind.PRODUCER and s.name == f"{queue} send"]
    receives = [s for s in finished if s.kind == trace.SpanKind.CONSUMER and s.name == f"{queue} receive"]
    assert len(sends) == 1
    assert len(receives) == 2
    producer_span = sends[0]
    for span in receives:
        assert span.context.trace_id == producer_span.context.trace_id
        assert span.parent is not None and span.parent.span_id == producer_span.context.span_id
    assert producer_span.attributes is not None
    assert producer_span.attributes["messaging.rabbitmq.publish.status"] == "confirmed"


# ── alert rules ──────────────────────────────────────────────────────────


def _promtool(docker_cli: str, *args: str) -> subprocess.CompletedProcess[str]:
    deploy = _ROOT / "deploy" / "prometheus"
    return subprocess.run(  # noqa: S603 — fixed docker argv, no shell
        [
            docker_cli,
            "run",
            "--rm",
            "-v",
            f"{deploy}:/w:ro",
            "-w",
            "/w/tests",
            "--entrypoint",
            "promtool",
            "prom/prometheus:v3.5.0",
            *args,
        ],
        capture_output=True,
        text=True,
        timeout=300,
        check=False,
    )


def test_alert_rules_pass_promtool() -> None:
    docker_cli = shutil.which("docker")
    if docker_cli is None:
        pytest.skip("docker CLI not available: cannot run promtool")
    try:
        import docker  # type: ignore[import-untyped]

        docker.from_env().ping()
    except Exception:
        pytest.skip("Docker daemon not reachable: cannot run promtool")
    check = _promtool(docker_cli, "check", "rules", "../rabbitkit-alerts.yaml")
    assert check.returncode == 0, check.stdout + check.stderr
    assert "SUCCESS: 16 rules found" in check.stdout
    test = _promtool(docker_cli, "test", "rules", "rabbitkit-alerts.test.yaml")
    assert test.returncode == 0, test.stdout + test.stderr
    assert "SUCCESS" in test.stdout
