"""Real-broker coverage for the observability foundation (G-22, G-23, PUB-017, CON-005).

* publish outcomes are counted by status AND bounded reason; routing failures
  (returned / exchange_not_found) are counted by the broker itself; confirm
  latency is the send-to-confirm time (PrometheusCollector, private registry)
* a publish-only broker gets connection and channel gauges
* ``connection_up`` / ``broker_connected`` drop when the connections are
  killed and come back after the reconnect; the failure reason is counted
* failed reconnect attempts are counted per role while the broker is down
* ``channels_active`` stays bounded under concurrent publishing
* ``connection_blocked`` follows a disk or memory alarm, and
  ``connection_blocked_reason`` says which (async and sync)
* ``QueueMetricsPoller`` reports a quorum queue's members / online counts
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


_REASONS = ("disk", "memory", "other")

# (rabbitmqctl command raising the alarm, command clearing it, expected reason label)
_ALARMS = {
    "disk": (("set_disk_free_limit", "100000GB"), ("set_disk_free_limit", "50MB")),
    "memory": (("set_vm_memory_high_watermark", "absolute", "1MB"), ("set_vm_memory_high_watermark", "0.4")),
}


def _blocked_reasons(history: History) -> dict[str, float | None]:
    return {r: history.gauge("rabbitkit_connection_blocked_reason", reason=r) for r in _REASONS}


@pytest.mark.parametrize("alarm", ["disk", "memory"])
async def test_connection_blocked_gauge_follows_an_alarm_with_its_reason(
    own_rabbit: dict[str, Any], alarm: str
) -> None:
    """Before: the gauge said a connection was blocked, never why; telling a
    disk alarm from a memory alarm needed the broker's rabbitmq_prometheus
    plugin. aiormq only logged connection.blocked's reason."""
    from rabbitkit import AsyncBroker
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.middleware.metrics import MetricsMiddleware

    raise_cmd, clear_cmd = _ALARMS[alarm]
    history = History()
    broker = AsyncBroker(_config(own_rabbit), middlewares=[MetricsMiddleware(history)])
    async with asyncio.timeout(90), _started(broker):
        assert history.gauge("rabbitkit_connection_blocked") == 0.0
        trigger: asyncio.Task[Any] | None = None
        try:
            await actl(own_rabbit, *raise_cmd)
            # RabbitMQ blocks a connection when it next publishes.
            trigger = asyncio.create_task(broker.publish(MessageEnvelope(routing_key="", body=b"t"), timeout=20))
            await _until(lambda: history.gauge("rabbitkit_connection_blocked") == 1.0, 20, "connection.blocked")
            assert broker._transport.blocked_reason == f"low on {alarm}"
            assert _blocked_reasons(history) == {r: 1.0 if r == alarm else 0.0 for r in _REASONS}
        finally:
            await actl(own_rabbit, *clear_cmd)
        await _until(lambda: history.gauge("rabbitkit_connection_blocked") == 0.0, 30, "connection.unblocked")
        assert _blocked_reasons(history) == dict.fromkeys(_REASONS, 0.0)
        if trigger is not None:
            await trigger


async def test_sync_broker_reports_the_blocked_reason(own_rabbit: dict[str, Any]) -> None:
    """pika hands connection.blocked's reason to the sync transport.

    pika runs the blocked/unblocked callbacks from its I/O loop
    (``pump_idle()`` / the consume loop), not while a publish waits for its
    confirm, so this publisher sends without confirms: the publish that makes
    the broker block the connection returns at once and the next pump sees
    the alarm."""
    from rabbitkit import SyncBroker
    from rabbitkit.core.config import PublisherConfig
    from rabbitkit.core.types import MessageEnvelope, PublishStatus
    from rabbitkit.middleware.metrics import MetricsMiddleware

    history = History()
    broker = SyncBroker(
        _config(own_rabbit, publisher=PublisherConfig(confirm_delivery=False)), middlewares=[MetricsMiddleware(history)]
    )
    blocked_seen = threading.Event()
    cleared = threading.Event()
    seen: list[Any] = []

    def pump_until(predicate: Any, what: str) -> None:
        deadline = time.monotonic() + 30
        while not predicate():
            assert time.monotonic() < deadline, f"timed out waiting for {what}"
            broker.pump_idle(0.1)

    def run() -> None:
        broker.start()
        try:
            transport = broker._transport
            seen.append(broker.publish(MessageEnvelope(routing_key="", body=b"t")).status)
            pump_until(lambda: transport.is_blocked, "connection.blocked (sync)")
            seen.append(transport.blocked_reason)
            blocked_seen.set()
            assert cleared.wait(30)
            pump_until(lambda: not transport.is_blocked, "connection.unblocked (sync)")
            seen.append(transport.blocked_reason)
        finally:
            blocked_seen.set()
            broker.stop()

    runner: asyncio.Future[None] | None = None
    try:
        await actl(own_rabbit, *_ALARMS["disk"][0])
        runner = asyncio.ensure_future(asyncio.to_thread(run))
        assert await asyncio.to_thread(blocked_seen.wait, 40)
        assert _blocked_reasons(history) == {"disk": 1.0, "memory": 0.0, "other": 0.0}
        assert history.gauge("rabbitkit_connection_blocked") == 1.0
    finally:
        await actl(own_rabbit, *_ALARMS["disk"][1])
        cleared.set()
    assert runner is not None
    await asyncio.wait_for(runner, 60)
    assert seen == [PublishStatus.SENT, "low on disk", None]
    assert _blocked_reasons(history) == dict.fromkeys(_REASONS, 0.0)


# ── quorum health from the management API (QueueMetricsPoller) ───────────


def test_queue_metrics_poller_reports_quorum_members_on_a_single_node(rabbit_container: dict[str, Any]) -> None:
    """A single-node broker: a quorum queue has one member, online. Classic
    queues get no quorum gauges."""
    import pika

    from rabbitkit.management import ManagementConfig, RabbitManagementClient
    from rabbitkit.queue_metrics import QueueMetricsPoller

    quorum, classic = _name("qq"), _name("cq")
    conn = pika.BlockingConnection(pika.URLParameters(rabbit_container["url"]))
    try:
        channel = conn.channel()
        channel.queue_declare(quorum, durable=True, arguments={"x-queue-type": "quorum"})
        channel.queue_declare(classic, durable=True)
        client = RabbitManagementClient(
            ManagementConfig(url=rabbit_container["mgmt"], username="guest", password="guest")
        )
        history = History()
        poller = QueueMetricsPoller(client, history, queue_filter=lambda q: q in (quorum, classic))
        deadline = time.monotonic() + 30
        while history.gauge("rabbitkit_queue_quorum_online", queue=quorum) is None:
            assert time.monotonic() < deadline, "the management API never listed the quorum queue"
            poller.poll_once()
            time.sleep(0.5)
        assert history.gauge("rabbitkit_queue_quorum_members", queue=quorum) == 1.0
        assert history.gauge("rabbitkit_queue_quorum_online", queue=quorum) == 1.0
        assert history.gauge("rabbitkit_queue_consumers", queue=classic) == 0.0  # the classic queue is polled
        assert history.gauge("rabbitkit_queue_quorum_members", queue=classic) is None
        channel.queue_delete(quorum)
        channel.queue_delete(classic)
    finally:
        conn.close()


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
    assert "SUCCESS: 27 rules found" in check.stdout
    test = _promtool(docker_cli, "test", "rules", "rabbitkit-alerts.test.yaml")
    assert test.returncode == 0, test.stdout + test.stderr
    assert "SUCCESS" in test.stdout
