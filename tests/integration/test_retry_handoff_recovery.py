"""Real-broker coverage for retry-handoff outages and their recovery (0.20.4).

The outage is the one seen in production-like runs: every ``<q>.retry.N``
queue refuses publishes (a policy ``max-length: 0, overflow: reject-publish``,
so the broker nacks each retry handoff) while the handler fails for a
retryable reason (its downstream is down).

* Deliveries are held, never lost: ready + unacked stays the published count,
  unacked never exceeds the prefetch of the consumers, nothing reaches a delay
  queue, and an async route does not hot-loop.
* After recovery every delivery is processed and acked.
* ``retry_handoff_paused`` returns to 0 although no retry handoff ever
  succeeds again (the handler recovered first). Before 0.20.4 it stayed 1.
* A new failure against a still-broken retry queue sets it to 1 again and
  starts a new streak (DEGRADED, not EXHAUSTED at once).
* Connection loss and stop() during the hold hand the deliveries back.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import threading
import time
import uuid
from collections.abc import Callable, Iterator
from typing import Any

import pytest

from rabbitkit.core.message import RabbitMessage  # resolved at runtime by the handlers' DI
from tests.integration.conftest import await_consumers, await_counts, ctl_queue_counts, live_counts

pytestmark = pytest.mark.integration

#: Fast handoff brake: holds of 0.2, 0.4, 0.8 then 1.0 s; EXHAUSTED after 5
#: failures; a streak is over after recovery_deadline + backoff_max = 3 s.
QUIET = 3.0


@pytest.fixture(scope="module")
def rabbit(rabbit_container: dict[str, Any]) -> dict[str, Any]:
    return rabbit_container


def _name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _retry(**handoff: float) -> Any:
    from rabbitkit.core.config import RetryConfig, RetryHandoffConfig

    params: dict[str, float] = {
        "backoff_initial": 0.2,
        "backoff_max": 1.0,
        "jitter": 0.0,
        "max_consecutive_failures": 5,
        "recovery_deadline": 2.0,
    }
    params.update(handoff)
    return RetryConfig(max_retries=2, delays=(1, 1), handoff=RetryHandoffConfig(**params))  # type: ignore[arg-type]


def _config(rabbit: dict[str, Any], *, connection_name: str | None = None, prefetch: int = 4) -> Any:
    import dataclasses

    from rabbitkit.core.config import ConnectionConfig, ConsumerConfig, RabbitConfig

    connection = dataclasses.replace(
        ConnectionConfig.from_url(rabbit["url"]), reconnect_backoff_base=0.2, reconnect_backoff_max=1.0
    )
    if connection_name is not None:
        connection = dataclasses.replace(connection, connection_name=connection_name)
    return RabbitConfig(connection=connection, consumer=ConsumerConfig(prefetch_count=prefetch))


class _Collector:
    """Thread-safe recording collector (sync routes emit from worker threads)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self.gauges: dict[tuple[str, tuple[tuple[str, str], ...]], float] = {}

    def inc_counter(self, name: str, labels: dict[str, str], value: float = 1.0) -> None:
        pass

    def observe_histogram(self, name: str, labels: dict[str, str], value: float) -> None:
        pass

    def set_gauge(self, name: str, labels: dict[str, str], value: float) -> None:
        with self._lock:
            self.gauges[(name, tuple(sorted(labels.items())))] = value

    def paused(self, queue: str) -> float | None:
        from rabbitkit.core.config import MetricsConfig

        with self._lock:
            return self.gauges.get((MetricsConfig().retry_handoff_paused, (("queue", queue),)))


def _ctl(rabbit: dict[str, Any], *args: str) -> str:
    result = rabbit["container"].get_wrapped_container().exec_run(["rabbitmqctl", *args])
    output = result[1] if isinstance(result, tuple) else result.output
    return output.decode("utf-8", "replace") if isinstance(output, bytes) else str(output)


def _block_retry_queues(rabbit: dict[str, Any], queue: str) -> str:
    """Every ``<queue>.retry.N`` refuses publishes (the broker nacks them)."""
    policy = f"block-{queue}"
    definition = json.dumps({"max-length": 0, "overflow": "reject-publish"})
    out = _ctl(
        rabbit, "set_policy", "--apply-to", "queues", "--priority", "10", policy, f"^{queue}\\.retry\\.", definition
    )
    assert "Setting policy" in out, out
    return policy


def _unblock(rabbit: dict[str, Any], policy: str) -> None:
    _ctl(rabbit, "clear_policy", policy)


def _publish(rabbit: dict[str, Any], queue: str, ids: list[str]) -> None:
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(rabbit["url"]))
    try:
        channel = conn.channel()
        channel.confirm_delivery()
        for message_id in ids:
            channel.basic_publish("", queue, b"{}", pika.BasicProperties(delivery_mode=2, message_id=message_id))
    finally:
        with contextlib.suppress(Exception):
            conn.close()


def _delete(rabbit: dict[str, Any], queue: str) -> None:
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(rabbit["url"]))
    try:
        channel = conn.channel()
        for name in (queue, f"{queue}.dlq", f"{queue}.retry.1", f"{queue}.retry.2"):
            with contextlib.suppress(Exception):
                channel.queue_delete(name)
    finally:
        with contextlib.suppress(Exception):
            conn.close()


async def _until(predicate: Callable[[], bool], timeout: float, what: str) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while not predicate():
        if loop.time() > deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for {what}")
        await asyncio.sleep(0.05)


def _retry_mw(broker: Any) -> Any:
    from rabbitkit.middleware.retry import RetryMiddleware

    (route,) = broker.routes
    return next(mw for mw in route.route_middlewares if isinstance(mw, RetryMiddleware))


def _assert_held_not_lost(rabbit: dict[str, Any], queue: str, published: int, max_unacked: int) -> None:
    counts = ctl_queue_counts(rabbit)
    ready, unacked = counts[queue]
    assert ready + unacked == published, counts[queue]
    assert unacked <= max_unacked, counts[queue]
    for tier in (1, 2):
        assert counts.get(f"{queue}.retry.{tier}", (0, 0)) == (0, 0), counts
    assert counts.get(f"{queue}.dlq", (0, 0)) == (0, 0), counts


# ── async: two instances, retry queues refused, then restored ──────────────


@pytest.mark.asyncio
async def test_async_outage_holds_deliveries_and_the_gauge_clears_after_recovery(
    rabbit: dict[str, Any], caplog: pytest.LogCaptureFixture
) -> None:
    from rabbitkit import AsyncBroker, RabbitQueue
    from rabbitkit.core.types import HandoffState
    from rabbitkit.middleware.metrics import MetricsMiddleware

    q = _name("rk-outage")
    state = {"down": True}
    processed: set[str] = set()
    calls = [0]
    collectors = [_Collector(), _Collector()]
    brokers: list[Any] = []
    for collector in collectors:
        broker = AsyncBroker(_config(rabbit, prefetch=4))
        broker._HEARTBEAT_INTERVAL = 0.2  # the periodic refresh, fast for the test

        @broker.subscriber(RabbitQueue(q), retry=_retry(), middlewares=[MetricsMiddleware(collector)])
        async def handle(body: bytes, message: RabbitMessage) -> None:
            calls[0] += 1
            if state["down"]:
                raise TimeoutError("downstream down")  # retryable
            processed.add(message.message_id)

        brokers.append(broker)

    policy = _block_retry_queues(rabbit, q)
    ids = [f"m{i}" for i in range(10)]
    try:
        async with asyncio.timeout(120):
            for broker in brokers:
                await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q, count=2)
            middlewares = [_retry_mw(b) for b in brokers]
            await asyncio.to_thread(_publish, rabbit, q, ids)

            # 1. The outage: every handoff is refused; deliveries are held, not lost.
            await _until(lambda: all(mw.handoff_tracker.is_exhausted for mw in middlewares), 30, "EXHAUSTED")
            assert [c.paused(q) for c in collectors] == [1.0, 1.0]
            await asyncio.to_thread(_assert_held_not_lost, rabbit, q, 10, 8)
            before = calls[0]
            await asyncio.sleep(2.0)
            # EXHAUSTED holds each failing delivery backoff_max (1 s): at most
            # prefetch x instances handler runs per hold, not a hot loop.
            assert calls[0] - before <= 8 * 3, calls[0] - before
            await asyncio.to_thread(_assert_held_not_lost, rabbit, q, 10, 8)

            # 2. The downstream recovers first; the retry queues still refuse.
            state["down"] = False
            await _until(lambda: processed == set(ids), 30, "every delivery processed")
            await await_counts(rabbit, q, (0, 0), timeout=30)

            # 3. Idle, and no retry handoff ever succeeded: the streak ends
            #    after the quiet period and the gauge returns to 0.
            await _until(lambda: [c.paused(q) for c in collectors] == [0.0, 0.0], QUIET + 10, "gauge 0")
            assert all(mw.handoff_tracker.state is HandoffState.HEALTHY for mw in middlewares)

            # 4. A new failure against the still-broken retry queue is not
            #    hidden: gauge 1 again, a NEW streak (DEGRADED, consecutive 1).
            caplog.clear()
            state["down"] = True
            with caplog.at_level(logging.WARNING, logger="rabbitkit.middleware.retry"):
                await asyncio.to_thread(_publish, rabbit, q, ["late"])
                await _until(lambda: 1.0 in [c.paused(q) for c in collectors], 20, "gauge 1 again")
            failures = [r.getMessage() for r in caplog.records if r.getMessage().startswith("Retry handoff failed")]
            assert failures and failures[0].startswith("Retry handoff failed (consecutive=1, state=degraded"), failures

            # 5. Full recovery.
            await asyncio.to_thread(_unblock, rabbit, policy)
            state["down"] = False
            await _until(lambda: "late" in processed, 30, "the late delivery processed")
            await await_counts(rabbit, q, (0, 0), timeout=30)
            for broker in brokers:
                await broker.stop()
            brokers.clear()
            assert [c.paused(q) for c in collectors] == [0.0, 0.0]  # nothing left at 1 after shutdown
    finally:
        for broker in brokers:
            await broker.stop()
        _unblock(rabbit, policy)
        _delete(rabbit, q)


# ── async: the retry topology stays broken → the gauge stays 1 ─────────────


@pytest.mark.asyncio
async def test_async_gauge_stays_1_while_the_retry_topology_stays_broken(rabbit: dict[str, Any]) -> None:
    """The retry queues refuse publishes for more than twice the quiet period.
    One delivery keeps failing (it needs a retry it cannot get) while other
    deliveries succeed in the handler. Sampled every 50 ms, through the
    heartbeat refreshes and the successful deliveries, the gauge never reads 0
    and the streak still becomes EXHAUSTED."""
    from rabbitkit import AsyncBroker, RabbitQueue
    from rabbitkit.middleware.metrics import MetricsMiddleware

    q = _name("rk-broken")
    collector = _Collector()
    succeeded: list[str] = []
    broker = AsyncBroker(_config(rabbit, prefetch=4))
    broker._HEARTBEAT_INTERVAL = 0.2  # refresh often: it must still not clear the gauge

    @broker.subscriber(RabbitQueue(q), retry=_retry(), middlewares=[MetricsMiddleware(collector)])
    async def handle(body: bytes, message: RabbitMessage) -> None:
        if (message.message_id or "").startswith("bad"):
            raise TimeoutError("downstream down")
        succeeded.append(message.message_id or "")

    policy = _block_retry_queues(rabbit, q)
    try:
        async with asyncio.timeout(60):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            mw = _retry_mw(broker)
            await asyncio.to_thread(_publish, rabbit, q, ["bad-1"])
            await _until(lambda: collector.paused(q) == 1.0, 20, "the first refused handoff")
            samples: list[float | None] = []
            loop = asyncio.get_running_loop()
            end = loop.time() + 2 * QUIET + 1.0
            good = 0
            while loop.time() < end:
                if good < 40:  # successful deliveries between the failing ones
                    await asyncio.to_thread(_publish, rabbit, q, [f"good-{good}"])
                    good += 1
                samples.append(collector.paused(q))
                await asyncio.sleep(0.05)
            assert samples and set(samples) == {1.0}, sorted(set(map(str, samples)))
            assert len(succeeded) == good  # the other deliveries kept flowing
            assert mw.handoff_tracker.is_exhausted
            await asyncio.to_thread(_assert_held_not_lost, rabbit, q, 1, 1)
    finally:
        await broker.stop()
        _unblock(rabbit, policy)
        _delete(rabbit, q)


# ── async: the connection is lost while deliveries are held ────────────────


def _kill_connections(rabbit: dict[str, Any], connection_name: str) -> int:
    from tests.integration.test_consumer_lifecycle import _kill_connections as kill

    return kill(rabbit, connection_name)


@pytest.mark.asyncio
async def test_async_connection_loss_during_the_hold_hands_deliveries_back(rabbit: dict[str, Any]) -> None:
    """The connection dies while four deliveries are held. aiormq cancels the
    held handlers; their nack fails on the dead channel (logged by the
    pipeline as an exception after settlement) and the broker requeues them.
    After the reconnect they are delivered again; nothing is lost or left held."""
    from rabbitkit import AsyncBroker, RabbitQueue

    q = _name("rk-hold-conn")
    connection_name = _name("conn")
    state = {"down": True}
    processed: set[str] = set()
    broker = AsyncBroker(_config(rabbit, connection_name=connection_name, prefetch=4))

    # A long hold, so the connection loss lands inside it.
    @broker.subscriber(RabbitQueue(q), retry=_retry(backoff_initial=10.0, backoff_max=10.0, recovery_deadline=60.0))
    async def handle(body: bytes, message: RabbitMessage) -> None:
        if state["down"]:
            raise TimeoutError("downstream down")
        processed.add(message.message_id)

    policy = _block_retry_queues(rabbit, q)
    ids = [f"c{i}" for i in range(4)]
    try:
        async with asyncio.timeout(90):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            mw = _retry_mw(broker)
            await asyncio.to_thread(_publish, rabbit, q, ids)
            await _until(lambda: mw.handoff_tracker.total_failures >= 4, 20, "four held deliveries")
            assert await asyncio.to_thread(live_counts, rabbit, q) == (0, 4)

            killed = await asyncio.to_thread(_kill_connections, rabbit, connection_name)
            assert killed > 0
            # The broker requeues the held deliveries of the dead channel;
            # after the reconnect they are delivered again (and held again).
            await await_consumers(rabbit["url"], q, timeout=30)
            state["down"] = False
            await asyncio.to_thread(_unblock, rabbit, policy)
            await _until(lambda: processed == set(ids), 40, "every delivery processed after the reconnect")
            await await_counts(rabbit, q, (0, 0), timeout=30)
            await _until(lambda: broker._in_flight == 0, 20, "no handler left holding a delivery")
    finally:
        await broker.stop()
        _unblock(rabbit, policy)
        _delete(rabbit, q)


# ── async: stop() during the hold ─────────────────────────────────────────


@pytest.mark.asyncio
async def test_async_stop_during_the_hold_requeues_and_zeroes_the_gauge(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker, RabbitQueue
    from rabbitkit.middleware.metrics import MetricsMiddleware

    q = _name("rk-hold-stop")
    collector = _Collector()
    broker = AsyncBroker(_config(rabbit, prefetch=3))

    @broker.subscriber(
        RabbitQueue(q),
        retry=_retry(backoff_initial=30.0, backoff_max=30.0, recovery_deadline=600.0),
        middlewares=[MetricsMiddleware(collector)],
    )
    async def handle(body: bytes) -> None:
        raise TimeoutError("downstream down")

    policy = _block_retry_queues(rabbit, q)
    try:
        async with asyncio.timeout(60):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            mw = _retry_mw(broker)
            await asyncio.to_thread(_publish, rabbit, q, ["s0", "s1", "s2"])
            await _until(lambda: mw.handoff_tracker.total_failures >= 3, 20, "three held deliveries")
            assert collector.paused(q) == 1.0
            started = time.monotonic()
            await broker.stop(timeout=1.0)
            assert time.monotonic() - started < 10  # the 30 s holds do not delay stop
        await await_counts(rabbit, q, (3, 0), timeout=20)  # handed back, none lost, none held
        assert collector.paused(q) == 0.0
        assert mw.handoff_tracker.total_failures == 3  # no redelivery loop during the stop
    finally:
        await broker.stop()
        _unblock(rabbit, policy)
        _delete(rabbit, q)


# ── async: the broker cancels the consumer during the hold ───────────────


def _unacked_on_node(rabbit: dict[str, Any]) -> int:
    out = _ctl(rabbit, "list_channels", "--no-table-headers", "messages_unacknowledged")
    return sum(int(line) for line in out.split() if line.isdigit())


@pytest.mark.asyncio
async def test_async_consumer_cancelled_during_the_hold_still_settles(rabbit: dict[str, Any]) -> None:
    """The source queue is deleted while two deliveries are held: the broker
    cancels the consumer (basic.cancel) but the deliveries stay on the open
    channel. The hold ends with their nack; nothing is left unacknowledged."""
    from rabbitkit import AsyncBroker, RabbitQueue

    q = _name("rk-hold-cancel")
    broker = AsyncBroker(_config(rabbit, prefetch=2))

    @broker.subscriber(RabbitQueue(q), retry=_retry(backoff_initial=3.0, backoff_max=3.0, recovery_deadline=60.0))
    async def handle(body: bytes) -> None:
        raise TimeoutError("downstream down")

    policy = _block_retry_queues(rabbit, q)
    try:
        async with asyncio.timeout(60):
            await broker.start(install_signal_handlers=False)
            await await_consumers(rabbit["url"], q)
            mw = _retry_mw(broker)
            await asyncio.to_thread(_publish, rabbit, q, ["x0", "x1"])
            await _until(lambda: mw.handoff_tracker.total_failures >= 2, 20, "two held deliveries")
            assert broker._in_flight == 2
            await asyncio.to_thread(_delete, rabbit, q)  # basic.cancel to our consumer
            (route,) = broker.routes
            await _until(lambda: route.runtime_state.consumer_cancelled, 20, "the broker's basic.cancel")
            await _until(lambda: broker._in_flight == 0, 20, "the held deliveries settled")
            assert await asyncio.to_thread(_unacked_on_node, rabbit) == 0
            assert mw.handoff_tracker.total_failures == 2  # no redelivery loop: the queue is gone
    finally:
        await broker.stop()
        _unblock(rabbit, policy)
        _delete(rabbit, q)


# ── sync: retry queues refused, then restored ─────────────────────────────


@contextlib.contextmanager
def _running(broker: Any) -> Iterator[None]:
    """``broker.run()`` on its own owner thread, stopped on exit."""
    failure: list[BaseException] = []

    def run() -> None:
        try:
            broker.run()
        except BaseException as exc:  # surfaced to the test
            failure.append(exc)

    runner = threading.Thread(target=run, name="sync-broker-owner", daemon=True)
    runner.start()
    deadline = time.monotonic() + 30
    while not failure and not getattr(broker._transport, "_consuming", False):
        assert time.monotonic() < deadline, "the consume loop never started"
        time.sleep(0.05)
    assert not failure, failure
    try:
        yield
    finally:
        broker._transport.stop_consuming()
        runner.join(timeout=30)
        assert not runner.is_alive(), "SyncBroker.run() did not return"
        assert not failure, failure


def _sync_until(predicate: Callable[[], bool], timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, f"timed out after {timeout}s waiting for {what}"
        time.sleep(0.05)


def test_sync_outage_holds_deliveries_and_the_gauge_clears_after_recovery(rabbit: dict[str, Any]) -> None:
    from rabbitkit import RabbitQueue, SyncBroker
    from rabbitkit.core.types import HandoffState
    from rabbitkit.middleware.metrics import MetricsMiddleware

    q = _name("rk-sync-outage")
    state = {"down": True}
    processed: set[str] = set()
    collector = _Collector()
    broker = SyncBroker(_config(rabbit, prefetch=4))
    broker._METRICS_SAMPLE_INTERVAL = 0.2  # the periodic refresh, fast for the test

    @broker.subscriber(RabbitQueue(q), retry=_retry(), middlewares=[MetricsMiddleware(collector)])
    def handle(body: bytes, message: RabbitMessage) -> None:
        if state["down"]:
            raise TimeoutError("downstream down")
        processed.add(message.message_id)

    policy = _block_retry_queues(rabbit, q)
    ids = [f"s{i}" for i in range(6)]
    try:
        with _running(broker):
            mw = _retry_mw(broker)
            _publish(rabbit, q, ids)
            # The sync path never sleeps on the I/O thread: refused handoffs are
            # nacked at once (requeued) and come straight back.
            _sync_until(lambda: mw.handoff_tracker.is_exhausted, 30, "EXHAUSTED")
            assert collector.paused(q) == 1.0
            _assert_held_not_lost(rabbit, q, 6, 4)

            state["down"] = False
            _sync_until(lambda: processed == set(ids), 30, "every delivery processed")
            _sync_until(lambda: live_counts(rabbit, q) == (0, 0), 30, "the queue drained")
            _sync_until(lambda: collector.paused(q) == 0.0, QUIET + 10, "gauge 0")
            assert mw.handoff_tracker.state is HandoffState.HEALTHY
        assert collector.paused(q) == 0.0
    finally:
        with contextlib.suppress(Exception):
            broker.stop()
        _unblock(rabbit, policy)
        _delete(rabbit, q)
