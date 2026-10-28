"""Real-broker tests for ``PublisherConfig.max_in_flight_bytes`` on SyncBroker.

* The budget bounds the bytes of publishes in flight: worker-thread handler
  publishes that would exceed it wait for room, every publish is still
  CONFIRMED, and the broker holds every message. ``publish_in_flight_bytes``
  is sampled from the sync transport.
* The owner-thread hazard: worker threads' publishes run on the connection's
  owner (I/O) thread. If the owner thread publishes while a worker holds the
  bytes it needs, and it slept waiting for them, the worker's publish could
  never run (only the owner runs it) and both would wait until a timeout.
  The owner keeps the I/O loop running while it waits, so both publishes
  complete at once.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest

from tests.integration.conftest import QueueProbe, wait_for_consumers
from tests.integration.test_observability_foundation import History
from tests.integration.test_sync_parity import _config, _name, _publish, _running, _until, _with_channel

pytestmark = pytest.mark.integration

_BODY = 1000


@pytest.fixture(scope="module")
def rabbit(rabbit_container: dict[str, Any]) -> dict[str, Any]:
    return rabbit_container


def _budget_config(url: str, budget: int, admission_timeout: float = 10.0) -> Any:
    from rabbitkit.core.config import PoolConfig, PublisherConfig

    return _config(
        url,
        publisher=PublisherConfig(max_message_bytes=_BODY, max_in_flight_bytes=budget),
        pool=PoolConfig(channel_acquire_timeout=admission_timeout),
    )


def _record_peak(limiter: Any) -> tuple[list[int], list[int]]:
    """Wrap ``limiter.acquire``: (bytes held right after each admission,
    one entry per publish that found no room and had to wait)."""
    peaks: list[int] = []
    waited: list[int] = []
    lock = threading.Lock()
    original = limiter.acquire

    def acquire(n: int, timeout: float, **kw: Any) -> bool:
        with lock:
            if limiter._waiters or limiter.used + n > limiter.limit:
                waited.append(n)
        ok: bool = original(n, timeout, **kw)
        if ok:
            with lock:
                peaks.append(limiter.used)
        return ok

    limiter.acquire = acquire
    return peaks, waited


def test_the_budget_bounds_bytes_in_flight_on_the_sync_broker(rabbit: dict[str, Any]) -> None:
    from rabbitkit import SyncBroker
    from rabbitkit.core.config import WorkerConfig
    from rabbitkit.middleware.metrics import MetricsMiddleware

    workers, rounds, budget = 4, 5, 2 * _BODY
    history = History()
    queue, out = _name("bytes"), _name("bytes-out")
    broker = SyncBroker(_budget_config(rabbit["url"], budget), middlewares=[MetricsMiddleware(history)])
    broker._METRICS_SAMPLE_INTERVAL = 0.0  # sample on every I/O tick
    barrier = threading.Barrier(workers)
    outcomes: list[Any] = []
    lock = threading.Lock()

    @broker.subscriber(queue, reject_without_dlx="discard")
    def handler(body: bytes) -> None:
        # Publish in bursts of `workers` at once: more bytes than the budget.
        try:
            barrier.wait(timeout=10)
        except threading.BrokenBarrierError:
            pass
        outcome = broker.publish(routing_key=out, body=b"r" * _BODY)
        with lock:
            outcomes.append(outcome)

    _with_channel(rabbit["url"], lambda ch: ch.queue_declare(out, durable=True))
    total = workers * rounds
    with _running(broker, WorkerConfig(worker_count=workers)):
        wait_for_consumers(rabbit["url"], queue)
        limiter = broker._transport.byte_limiter
        assert limiter is not None and limiter.limit == budget
        peaks, waited = _record_peak(limiter)
        _publish(rabbit["url"], queue, *[b"m"] * total)
        _until(lambda: len(outcomes) == total, 60, "every handler publish")
        _until(lambda: history.gauge("rabbitkit_publish_in_flight_bytes") == 0.0, 20, "the bytes gauge back at 0")

    assert [o.status.value for o in outcomes] == ["confirmed"] * total
    assert max(peaks) <= budget  # never more than the budget in flight
    assert waited, "no publish had to wait: the test did not exercise the budget"
    assert max(history.gauge_history("rabbitkit_publish_in_flight_bytes")) >= _BODY
    with QueueProbe(rabbit["url"]) as probe:
        assert probe.ready(out) == total


def test_an_owner_thread_publish_waiting_for_budget_runs_the_workers_publishes(rabbit: dict[str, Any]) -> None:
    """The deadlock scenario. The budget fits one message. Each round a worker
    handler's publish takes the whole budget and waits for the owner thread
    to run it; the owner thread (an I/O-tick hook, as rabbitkit's own
    deadline settlement is) then publishes too and must wait for that same
    budget. Without pumping the owner would sleep until the admission timeout
    while the worker's publish could never run."""
    from rabbitkit import SyncBroker
    from rabbitkit.core.config import WorkerConfig

    rounds, admission_timeout = 5, 10.0
    queue, out = _name("owner"), _name("owner-out")
    broker = SyncBroker(_budget_config(rabbit["url"], _BODY, admission_timeout))
    worker_outcomes: list[Any] = []
    owner_outcomes: list[tuple[Any, float]] = []
    contended: list[bool] = []
    armed = threading.Event()

    @broker.subscriber(queue, reject_without_dlx="discard")
    def handler(body: bytes) -> None:
        worker_outcomes.append(broker.publish(routing_key=out, body=b"w" * _BODY))

    def owner_publishes_while_a_worker_holds_the_budget() -> None:
        if not armed.is_set():
            return
        limiter = broker._transport.byte_limiter
        deadline = time.monotonic() + 0.2
        while limiter.used == 0 and time.monotonic() < deadline:
            time.sleep(0.001)
        if limiter.used == 0:
            return  # the delivery has not reached a worker yet: next tick
        armed.clear()
        contended.append(broker._transport.publishes_in_flight >= 1)
        started = time.monotonic()
        outcome = broker.publish(routing_key=out, body=b"o" * _BODY)
        owner_outcomes.append((outcome, time.monotonic() - started))

    _with_channel(rabbit["url"], lambda ch: ch.queue_declare(out, durable=True))
    with _running(broker, WorkerConfig(worker_count=2)):
        wait_for_consumers(rabbit["url"], queue)
        broker._transport.on_io_tick(owner_publishes_while_a_worker_holds_the_budget)
        for i in range(rounds):
            armed.set()
            _publish(rabbit["url"], queue, b"m")
            _until(lambda i=i: len(worker_outcomes) == i + 1 and len(owner_outcomes) == i + 1, 30, f"round {i}")

    assert len(contended) == rounds and all(contended)
    assert [o.status.value for o in worker_outcomes] == ["confirmed"] * rounds
    assert [o.status.value for o, _ in owner_outcomes] == ["confirmed"] * rounds
    # Far below the admission timeout and the worker's 5 s marshal bound.
    assert max(seconds for _, seconds in owner_outcomes) < 2.0
    with QueueProbe(rabbit["url"]) as probe:
        assert probe.ready(out) == 2 * rounds
