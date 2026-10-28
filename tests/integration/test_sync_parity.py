"""Real-broker tests for SyncBroker parity with AsyncBroker.

Each test asserts what the BROKER reports (queue counts, consumer counts),
not only what the client believes it did:

* a queue deleted under a running SyncBroker consumer is
  reported as not consuming; recreating it resumes consumption, on the
  consume loop's own thread, without rabbitkit re-creating the queue.
* ``ConsumerConfig.processing_timeout`` settles a
  delivery whose handler sleeps past it (requeue, then DLQ on the
  redelivery); the late handler return settles nothing a second time and
  the channel stays open.
* ``publish_in_flight`` is sampled from the sync transport.
"""

from __future__ import annotations

import contextlib
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from tests.integration.conftest import QueueProbe, wait_for_consumers, wait_for_counts
from tests.integration.test_broker_metrics import History

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def rabbit(rabbit_container: dict[str, Any]) -> dict[str, Any]:
    return rabbit_container


def _config(url: str, **kw: Any) -> Any:
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    return RabbitConfig(connection=ConnectionConfig.from_url(url), **kw)


def _name(prefix: str) -> str:
    return f"syncpar-{prefix}-{uuid.uuid4().hex[:8]}"


def _with_channel(url: str, fn: Callable[[Any], Any]) -> Any:
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(url))
    try:
        ch = conn.channel()
        ch.confirm_delivery()
        return fn(ch)
    finally:
        conn.close()


def _publish(url: str, queue: str, *bodies: bytes) -> None:
    import pika

    def send(ch: Any) -> None:
        for body in bodies:
            ch.basic_publish(exchange="", routing_key=queue, body=body, properties=pika.BasicProperties(message_id="m"))

    _with_channel(url, send)


def _until(predicate: Callable[[], bool], timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        time.sleep(0.05)


@contextlib.contextmanager
def _running(broker: Any, worker_config: Any = None) -> Iterator[None]:
    """``broker.run()`` on its own thread (the owner thread), stopped on exit."""
    failure: list[BaseException] = []

    def run() -> None:
        try:
            broker.run(worker_config=worker_config)
        except BaseException as exc:  # surfaced to the test below
            failure.append(exc)

    runner = threading.Thread(target=run, name="sync-broker-owner", daemon=True)
    runner.start()
    _until(lambda: bool(failure) or bool(getattr(broker._transport, "_consuming", False)), 30, "the consume loop")
    assert not failure, failure
    try:
        yield
    finally:
        broker._transport.stop_consuming()
        runner.join(timeout=30)
        assert not runner.is_alive(), "SyncBroker.run() did not return"
        assert not failure, failure


# ── broker-initiated basic.cancel ──────────────────────────────────


@pytest.mark.parametrize("workers", [1, 3], ids=["inline", "pool"])
def test_deleted_queue_reports_not_consuming_and_resumes_when_recreated(
    rabbit: dict[str, Any], monkeypatch: pytest.MonkeyPatch, workers: int
) -> None:
    from rabbitkit import SyncBroker
    from rabbitkit.core.config import WorkerConfig
    from rabbitkit.health import HealthStatus, broker_health_check, broker_readiness
    from rabbitkit.sync import consumers as consumers_mod

    monkeypatch.setattr(consumers_mod, "RESUBSCRIBE_BACKOFF_MAX", 2.0)
    queue = _name("g26")
    broker = SyncBroker(_config(rabbit["url"]))
    handled: list[bytes] = []
    events: list[tuple[str, str, str]] = []

    @broker.subscriber(queue, reject_without_dlx="discard")
    def handler(body: bytes) -> None:
        handled.append(body)

    broker.on_consumer_cancelled = lambda q: events.append(("cancelled", q, threading.current_thread().name))
    broker.on_consumer_resumed = lambda q: events.append(("resumed", q, threading.current_thread().name))

    with _running(broker, WorkerConfig(worker_count=workers)):
        wait_for_consumers(rabbit["url"], queue)
        assert broker_health_check(broker).status == HealthStatus.HEALTHY

        _with_channel(rabbit["url"], lambda ch: ch.queue_delete(queue))
        _until(lambda: broker_health_check(broker).consumer_count == 0, 20, "the cancel to be reported")
        result = broker_health_check(broker)
        assert result.status == HealthStatus.DEGRADED
        assert result.route_count == 1
        assert broker_readiness(broker) is False
        assert broker._transport.cancelled_consumers == frozenset({queue})
        assert events == [("cancelled", queue, "sync-broker-owner")]

        # The consume loop probes (passively) while it is gone; it never re-creates it.
        time.sleep(2.5)
        with QueueProbe(rabbit["url"]) as probe:
            assert probe.declare(queue) is None

        _with_channel(rabbit["url"], lambda ch: ch.queue_declare(queue, durable=True))
        wait_for_consumers(rabbit["url"], queue, timeout=30)
        _until(lambda: broker_health_check(broker).consumer_count == 1, 20, "the consumer to be reported back")
        assert events == [("cancelled", queue, "sync-broker-owner"), ("resumed", queue, "sync-broker-owner")]
        assert broker_readiness(broker) is True
        assert broker._transport.cancelled_consumers == frozenset()

        _publish(rabbit["url"], queue, b"after")
        _until(lambda: handled == [b"after"], 20, "the message published after the resume")
        assert wait_for_counts(rabbit, queue, (0, 0), timeout=20) == (0, 0)
        with QueueProbe(rabbit["url"]) as probe:
            assert probe.consumers(queue) == 1  # exactly one consumer


# ── processing_timeout on the sync broker ──────────────────


@pytest.mark.parametrize("workers", [1, 2], ids=["single-worker", "pool"])
def test_processing_timeout_requeues_then_dead_letters_and_ignores_the_late_return(
    rabbit: dict[str, Any], caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str], workers: int
) -> None:
    from rabbitkit import SyncBroker
    from rabbitkit.core.config import ConsumerConfig, WorkerConfig

    timeout, sleep = 0.4, 1.5
    queue = _name("deadline")
    broker = SyncBroker(_config(rabbit["url"], consumer=ConsumerConfig(processing_timeout=timeout)))
    runs: list[float] = []
    returned: list[bytes] = []
    lock = threading.Lock()

    @broker.subscriber(queue)  # reject_without_dlx="auto_provision": <queue>.dlq
    def handler(body: bytes) -> None:
        with lock:
            runs.append(time.monotonic())
        if body == b"slow":
            time.sleep(sleep)
        with lock:
            returned.append(body)

    caplog.set_level("WARNING")
    with _running(broker, WorkerConfig(worker_count=workers)):
        wait_for_consumers(rabbit["url"], queue)
        sent = time.monotonic()
        _publish(rabbit["url"], queue, b"slow")
        with QueueProbe(rabbit["url"]) as probe:
            _until(lambda: probe.ready(f"{queue}.dlq") == 1, 20, "the DLQ copy")
        to_dlq = time.monotonic() - sent
        assert len(runs) == 2  # first delivery requeued at the deadline, the redelivery dead-lettered
        # Within ~timeout of the second run's start (which waits for the slot
        # with one worker: the late handler keeps it until it returns).
        assert runs[1] - sent < (sleep if workers == 1 else timeout) + 1.0
        assert to_dlq < (runs[1] - sent) + timeout + 1.0

        # Both late returns happen; neither settles again: the consumer and
        # its channel survive (a second ack of a settled tag would close it).
        _until(lambda: returned.count(b"slow") == 2, 20, "the late handler returns")
        assert broker._transport.has_open_channels
        _publish(rabbit["url"], queue, b"fast")
        _until(lambda: b"fast" in returned, 20, "a later message")
        assert wait_for_counts(rabbit, queue, (0, 0), timeout=20) == (0, 0)
        with QueueProbe(rabbit["url"]) as probe:
            assert probe.consumers(queue) == 1
            assert probe.ready(f"{queue}.dlq") == 1
    text = caplog.text + capsys.readouterr().out  # stdlib or structlog rendering
    assert "exceeded processing_timeout" in text
    assert "error=HandlerTimeoutError" in text and "outcome_unknown=True" in text
    assert text.count("so its result is discarded") == 2  # one per late return


def test_processing_timeout_refuses_the_late_handlers_result_publish(rabbit: dict[str, Any]) -> None:
    """A handler with a result publisher returns after its deadline: the
    result is NOT published (its delivery was already requeued)."""
    from rabbitkit import RabbitMessage, SyncBroker
    from rabbitkit.core.config import ConsumerConfig, WorkerConfig

    queue, results = _name("late-result"), _name("results")
    broker = SyncBroker(_config(rabbit["url"], consumer=ConsumerConfig(processing_timeout=0.3)))
    done = threading.Event()

    @broker.subscriber(queue, reject_without_dlx="discard")
    @broker.publisher(routing_key=results)
    def handler(body: bytes, message: RabbitMessage) -> bytes:
        if not message.redelivered:
            time.sleep(1.0)
            done.set()
        return b"result-" + body

    _with_channel(rabbit["url"], lambda ch: ch.queue_declare(results, durable=True))
    with _running(broker, WorkerConfig(worker_count=2)):
        wait_for_consumers(rabbit["url"], queue)
        _publish(rabbit["url"], queue, b"x")
        assert done.wait(20)
        # The redelivery (handled in time) publishes one result; the late
        # first run publishes none.
        with QueueProbe(rabbit["url"]) as probe:
            _until(lambda: probe.ready(results) == 1, 20, "the redelivery's result")
            time.sleep(0.5)
            assert probe.ready(results) == 1
        assert wait_for_counts(rabbit, queue, (0, 0), timeout=20) == (0, 0)


# ── publish_in_flight on the sync broker ───────────────────────────


def test_publish_in_flight_is_sampled_from_the_sync_transport(rabbit: dict[str, Any]) -> None:
    """Worker-thread publishes wait for the I/O thread; the sampled gauge sees
    them, and goes back to 0 once they are confirmed."""
    from rabbitkit import SyncBroker
    from rabbitkit.core.config import WorkerConfig
    from rabbitkit.middleware.metrics import MetricsMiddleware

    history = History()
    queue, out = _name("inflight"), _name("out")
    broker = SyncBroker(_config(rabbit["url"]), middlewares=[MetricsMiddleware(history)])
    broker._METRICS_SAMPLE_INTERVAL = 0.0  # sample on every I/O tick
    outcomes: list[bool] = []

    @broker.subscriber(queue, reject_without_dlx="discard")
    def handler(body: bytes) -> None:
        outcomes.append(broker.publish(routing_key=out, body=body).ok)

    _with_channel(rabbit["url"], lambda ch: ch.queue_declare(out, durable=True))
    seen = threading.Event()

    def wait_for_a_waiting_publish() -> None:
        # Runs first on the I/O tick: hold the tick until a worker's publish
        # is queued for the I/O thread, so the sampler right after sees it.
        if seen.is_set():
            return
        transport = broker._transport
        deadline = time.monotonic() + 5
        while transport.publishes_in_flight == 0 and time.monotonic() < deadline:
            time.sleep(0.001)
        if transport.publishes_in_flight:
            seen.set()

    with _running(broker, WorkerConfig(worker_count=2)):
        wait_for_consumers(rabbit["url"], queue)
        broker._transport._io_tick_callbacks.insert(0, wait_for_a_waiting_publish)
        _publish(rabbit["url"], queue, b"a")
        assert seen.wait(20)
        _until(lambda: outcomes == [True], 20, "the handler's publish")
        _until(lambda: history.gauge("rabbitkit_publish_in_flight") == 0.0, 20, "the gauge back at 0")
        assert max(history.gauge_history("rabbitkit_publish_in_flight")) >= 1.0
    assert history.gauge("rabbitkit_publish_in_flight") == 0.0  # stop() zeroes it
