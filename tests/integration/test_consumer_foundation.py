"""Real-broker tests for the consumer foundation (wave 1, consumer stream).

Each test asserts what the BROKER reports (queue counts, consumer counts,
channel state), not only what the client believes it did:

* S4 / S5 repros from the Phase 2.1 audit (MANUAL failure stranding a
  prefetch slot; default AUTO transient hot loop) stay fixed.
* G-15: concurrent settlements of one delivery never close the channel.
* G-26: a queue deleted under a running consumer is reported as not
  consuming; recreating it resumes consumption.
* CONSUMER-007: a reconnect during ``stop()``'s drain does not bring a
  cancelled consumer back.
* CONSUMER-005: QoS is reapplied after a reconnect.
* §3.2: worker-pool deliveries that had not started are handed back at once
  by ``stop()`` while in-flight handlers keep their grace period.
"""

from __future__ import annotations

import asyncio
import contextlib
import uuid
from typing import Any

import pytest

from tests.integration.conftest import QueueProbe, await_consumers, await_counts, live_counts

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def rabbit(rabbit_container: dict[str, Any]) -> dict[str, Any]:
    return rabbit_container


def _config(url: str, **kw: Any) -> Any:
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    return RabbitConfig(connection=ConnectionConfig.from_url(url), **kw)


def _name(prefix: str) -> str:
    return f"cfound-{prefix}-{uuid.uuid4().hex[:8]}"


async def _publish_raw(url: str, queue: str, ids: list[str]) -> None:
    import aio_pika

    conn = await aio_pika.connect(url)
    async with conn:
        ch = await conn.channel(publisher_confirms=True)
        for mid in ids:
            await ch.default_exchange.publish(aio_pika.Message(body=b"{}", message_id=mid), routing_key=queue)


def _probe(url: str, queue: str) -> tuple[int | None, int | None]:
    """``(ready, consumers)`` by passive declare; ``(None, None)`` if the queue is absent."""
    with QueueProbe(url) as probe:
        method = probe.declare(queue)
    if method is None:
        return None, None
    return int(method.message_count), int(method.consumer_count)


async def _await_probe(url: str, queue: str, *, ready: int | None = None, consumers: int | None = None) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + 20
    seen: tuple[int | None, int | None] = (None, None)
    while loop.time() < deadline:
        seen = await asyncio.to_thread(_probe, url, queue)
        if (ready is None or seen[0] == ready) and (consumers is None or seen[1] == consumers):
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"{queue}: expected ready={ready} consumers={consumers}, last saw {seen}")


def _kill_connections(rabbit: dict[str, Any], *, at_least: int = 2) -> int:
    """Force-close every AMQP connection on the node (this suite is serial).

    The management API lists a new connection only after its next stats
    refresh, so poll until at least *at_least* connections are visible."""
    import time

    from rabbitkit.management import ManagementConfig, RabbitManagementClient

    client = RabbitManagementClient(ManagementConfig(url=rabbit["mgmt"], username="guest", password="guest"))
    deadline = time.monotonic() + 15
    names: list[str] = []
    while time.monotonic() < deadline:
        names = [str(c.get("name")) for c in client.list_connections() if c.get("name")]
        if len(names) >= at_least:
            break
        time.sleep(0.25)
    killed = 0
    for name in names:
        with contextlib.suppress(Exception):
            client.close_connection(name, reason="consumer foundation test")
            killed += 1
    return killed


async def _await_reconnected(broker: Any, timeout: float = 20.0) -> None:
    """Wait until the broker's connections dropped and came back."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    transport = broker._transport
    while transport.is_connected():
        if loop.time() > deadline:
            raise AssertionError("connections were never seen dropping")
        await asyncio.sleep(0.02)
    while not transport.is_connected():
        if loop.time() > deadline:
            raise AssertionError("connections never came back")
        await asyncio.sleep(0.05)


async def _delete_queue(url: str, queue: str) -> None:
    import aio_pika

    conn = await aio_pika.connect(url)
    async with conn:
        ch = await conn.channel()
        await ch.queue_delete(queue)


async def _declare_queue(url: str, queue: str) -> None:
    import aio_pika

    conn = await aio_pika.connect(url)
    async with conn:
        ch = await conn.channel()
        await ch.declare_queue(queue, durable=True)


# ── S4 / S5: the base's fixes hold on a real broker ──────────────────────


async def test_s4_manual_exception_does_not_strand_the_message(rabbit: dict[str, Any]) -> None:
    """Audit S4 verbatim: prefetch=1, MANUAL, the first message raises before
    settling. It used to hold the only prefetch slot, so message 2 was never
    delivered. Now the failure is dead-lettered and message 2 is handled."""
    from rabbitkit import AckPolicy, AsyncBroker, RabbitMessage
    from rabbitkit.core.config import ConsumerConfig

    queue = _name("s4")
    broker = AsyncBroker(_config(rabbit["url"], consumer=ConsumerConfig(prefetch_count=1)))
    seen: list[str] = []
    second = asyncio.Event()

    @broker.subscriber(queue, ack_policy=AckPolicy.MANUAL)
    async def handler(message: RabbitMessage) -> None:
        seen.append(message.message_id or "?")
        if message.message_id == "1":
            raise RuntimeError("boom before settling")
        await message.ack_async()
        second.set()

    await broker.start(install_signal_handlers=False)
    try:
        await await_consumers(rabbit["url"], queue)
        await _publish_raw(rabbit["url"], queue, ["1", "2"])
        await asyncio.wait_for(second.wait(), timeout=20)
        assert seen == ["1", "2"]
        await await_counts(rabbit, queue, (0, 0), timeout=20)
        await _await_probe(rabbit["url"], f"{queue}.dlq", ready=1)
    finally:
        await broker.stop(timeout=2)


async def test_s5_default_transient_failure_is_bounded(rabbit: dict[str, Any]) -> None:
    """Audit S5 verbatim: default config, handler always raises OSError. It
    used to run thousands of times a second; now it runs twice (first
    delivery requeued, redelivery dead-lettered)."""
    from rabbitkit import AsyncBroker, RabbitMessage

    queue = _name("s5")
    broker = AsyncBroker(_config(rabbit["url"]))
    runs: list[bool] = []

    @broker.subscriber(queue)
    async def handler(message: RabbitMessage) -> None:
        runs.append(message.redelivered)
        raise OSError("db down")

    await broker.start(install_signal_handlers=False)
    try:
        await await_consumers(rabbit["url"], queue)
        await _publish_raw(rabbit["url"], queue, ["x"])
        await _await_probe(rabbit["url"], f"{queue}.dlq", ready=1)
        await asyncio.sleep(1.0)  # would be thousands of extra runs under the old default
        assert runs == [False, True]
        await await_counts(rabbit, queue, (0, 0), timeout=20)
    finally:
        await broker.stop(timeout=2)


# ── G-15: concurrent settlement sends one frame per delivery ─────────────


async def test_concurrent_settlements_never_close_the_channel(rabbit: dict[str, Any]) -> None:
    """A MANUAL handler settles every delivery three ways at once. Two frames
    for one delivery tag make the broker close the channel with
    PRECONDITION_FAILED (unknown delivery tag) and redeliver everything
    unacked on it; one frame per delivery keeps the channel open and each
    message is handled exactly once."""
    from rabbitkit import AckPolicy, AsyncBroker, RabbitMessage
    from rabbitkit.core.config import ConsumerConfig

    queue = _name("g15")
    total = 20
    broker = AsyncBroker(_config(rabbit["url"], consumer=ConsumerConfig(prefetch_count=total)))
    handled: list[str] = []
    done = asyncio.Event()

    @broker.subscriber(queue, ack_policy=AckPolicy.MANUAL)
    async def handler(message: RabbitMessage) -> None:
        handled.append(message.message_id or "?")
        await asyncio.gather(
            message.ack_async(),
            message.nack_async(requeue=True),
            message.reject_async(requeue=False),
        )
        if len(handled) >= total:
            done.set()

    await broker.start(install_signal_handlers=False)
    try:
        await await_consumers(rabbit["url"], queue)
        channel = broker._transport._consumer_channels[queue]
        await _publish_raw(rabbit["url"], queue, [str(i) for i in range(total)])
        await asyncio.wait_for(done.wait(), timeout=20)
        await await_counts(rabbit, queue, (0, 0), timeout=20)
        await asyncio.sleep(0.5)  # a 406 would close the channel and redeliver
        assert not channel.is_closed
        assert sorted(handled, key=int) == [str(i) for i in range(total)]
        assert await asyncio.to_thread(_probe, rabbit["url"], f"{queue}.dlq") == (0, 0)
    finally:
        await broker.stop(timeout=2)


# ── G-26: broker-initiated consumer cancel ────────────────────────────────


@pytest.mark.parametrize("native_recovery", [True, False], ids=["with-aio-pika-recovery", "rabbitkit-only"])
async def test_deleted_queue_reports_not_consuming_and_resumes_when_recreated(
    rabbit: dict[str, Any], monkeypatch: pytest.MonkeyPatch, native_recovery: bool
) -> None:
    """Delete the queue under a running consumer: health reports the route as
    not consuming and the hook fires. Recreate it: the consumer comes back and
    a new message is handled.

    ``rabbitkit-only`` switches off aio-pika 10's own consumer-cancel recovery
    (when present) so rabbitkit's re-subscription is what is exercised."""
    import aio_pika

    from rabbitkit import AsyncBroker
    from rabbitkit.async_ import consumers as consumers_mod
    from rabbitkit.health import HealthStatus, broker_health_check, broker_readiness

    if not native_recovery and hasattr(aio_pika.RobustChannel, "_on_consumer_cancel"):
        monkeypatch.setattr(aio_pika.RobustChannel, "_on_consumer_cancel", lambda self, frame: None)
    monkeypatch.setattr(consumers_mod, "RESUBSCRIBE_BACKOFF_MAX", 2.0)

    queue = _name("g26")
    broker = AsyncBroker(_config(rabbit["url"]))
    handled: list[str] = []
    events: list[tuple[str, str]] = []
    got_after = asyncio.Event()

    @broker.subscriber(queue, reject_without_dlx="discard")
    async def handler(body: bytes) -> None:
        handled.append("x")
        got_after.set()

    broker.on_consumer_cancelled = lambda q: events.append(("cancelled", q))
    broker.on_consumer_resumed = lambda q: events.append(("resumed", q))

    await broker.start(install_signal_handlers=False)
    try:
        await await_consumers(rabbit["url"], queue)
        assert broker_health_check(broker).status == HealthStatus.HEALTHY

        await _delete_queue(rabbit["url"], queue)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + 20
        while broker_health_check(broker).consumer_count != 0:
            assert loop.time() < deadline, "health never reported the cancelled consumer"
            await asyncio.sleep(0.05)
        result = broker_health_check(broker)
        assert result.status == HealthStatus.DEGRADED
        assert result.route_count == 1
        assert broker_readiness(broker) is False
        assert events == [("cancelled", queue)]
        assert broker._transport.cancelled_consumers == frozenset({queue})

        await _declare_queue(rabbit["url"], queue)
        await await_consumers(rabbit["url"], queue, timeout=30)
        deadline = loop.time() + 20
        while broker_health_check(broker).consumer_count != 1:
            assert loop.time() < deadline, "health never reported the consumer back"
            await asyncio.sleep(0.05)
        assert events == [("cancelled", queue), ("resumed", queue)]
        assert broker_readiness(broker) is True

        await _publish_raw(rabbit["url"], queue, ["after"])
        await asyncio.wait_for(got_after.wait(), timeout=20)
        await await_counts(rabbit, queue, (0, 0), timeout=20)
        assert (await asyncio.to_thread(_probe, rabbit["url"], queue))[1] == 1  # exactly one consumer
    finally:
        await broker.stop(timeout=2)


# ── CONSUMER-007: no resurrection during the drain ───────────────────────


async def test_reconnect_during_drain_does_not_resubscribe_a_cancelled_consumer(rabbit: dict[str, Any]) -> None:
    """stop() cancels the consumer, then waits for an in-flight pooled
    handler. A connection kill during that wait used to let aio-pika's robust
    restore re-subscribe the cancelled consumer (the cancel went through a
    fresh queue object), which took new deliveries during shutdown."""
    from rabbitkit import AsyncBroker
    from rabbitkit.core.config import ConsumerConfig, WorkerConfig

    queue = _name("c007")
    broker = AsyncBroker(_config(rabbit["url"], consumer=ConsumerConfig(graceful_timeout=30)))
    started = asyncio.Event()
    release = asyncio.Event()
    calls: list[str] = []

    @broker.subscriber(queue, reject_without_dlx="discard")
    async def handler(body: bytes) -> None:
        calls.append("run")
        started.set()
        await release.wait()

    await broker.start(worker_config=WorkerConfig(worker_count=2), install_signal_handlers=False)
    stopping: asyncio.Task[None] | None = None
    try:
        await await_consumers(rabbit["url"], queue)
        await _publish_raw(rabbit["url"], queue, ["in-flight"])
        await asyncio.wait_for(started.wait(), timeout=20)

        stopping = asyncio.create_task(broker.stop())
        await _await_probe(rabbit["url"], queue, consumers=0)
        await _publish_raw(rabbit["url"], queue, ["during-drain"])

        assert await asyncio.to_thread(_kill_connections, rabbit) > 0
        await _await_reconnected(broker)
        await asyncio.sleep(3.0)  # aio-pika restores consumers right after reconnecting

        ready, consumers = await asyncio.to_thread(_probe, rabbit["url"], queue)
        assert consumers == 0, "a cancelled consumer was re-subscribed by the reconnect"
        assert ready == 2  # the in-flight delivery (its channel died) and the new one
        assert calls == ["run"]
    finally:
        release.set()
        if stopping is not None:
            await asyncio.wait_for(stopping, timeout=40)
        else:
            await broker.stop(timeout=2)
    await await_counts(rabbit, queue, (2, 0), timeout=20)


# ── CONSUMER-005: QoS survives a reconnect ───────────────────────────────


async def test_prefetch_is_reapplied_after_reconnect(rabbit: dict[str, Any]) -> None:
    """Per-route prefetch=2 and a handler that blocks: the broker holds 2
    unacked. After every connection is killed and restored, still 2, not the
    whole backlog."""
    from rabbitkit import AsyncBroker

    queue = _name("qos")
    broker = AsyncBroker(_config(rabbit["url"]))
    release = asyncio.Event()

    @broker.subscriber(queue, prefetch_count=2, reject_without_dlx="discard")
    async def handler(body: bytes) -> None:
        await release.wait()

    await broker.start(install_signal_handlers=False)
    try:
        await await_consumers(rabbit["url"], queue)
        await _publish_raw(rabbit["url"], queue, [str(i) for i in range(8)])
        await await_counts(rabbit, queue, (6, 2), timeout=20)

        assert await asyncio.to_thread(_kill_connections, rabbit) > 0
        await _await_reconnected(broker)
        await await_consumers(rabbit["url"], queue, timeout=30)
        await await_counts(rabbit, queue, (6, 2), timeout=20)
        await asyncio.sleep(1.0)
        assert await asyncio.to_thread(live_counts, rabbit, queue) == (6, 2)
    finally:
        release.set()
        await broker.stop(timeout=5)


# ── §3.2: not-started pool work is released at stop() ────────────────────


async def test_stop_releases_prefetched_pool_work_while_in_flight_keeps_its_grace(rabbit: dict[str, Any]) -> None:
    """worker_count=2, prefetch 10, six messages, handlers block: two run,
    four wait in the pool. stop() hands the four back at once (ready on the
    broker) while the two in flight keep running until released, then ack."""
    from rabbitkit import AsyncBroker
    from rabbitkit.core.config import ConsumerConfig, WorkerConfig

    queue = _name("pool")
    broker = AsyncBroker(_config(rabbit["url"], consumer=ConsumerConfig(prefetch_count=10, graceful_timeout=30)))
    release = asyncio.Event()
    running: list[str] = []

    @broker.subscriber(queue, reject_without_dlx="discard")
    async def handler(body: bytes) -> None:
        running.append("x")
        await release.wait()

    await broker.start(worker_config=WorkerConfig(worker_count=2), install_signal_handlers=False)
    stopping: asyncio.Task[None] | None = None
    try:
        await await_consumers(rabbit["url"], queue)
        await _publish_raw(rabbit["url"], queue, [str(i) for i in range(6)])
        await await_counts(rabbit, queue, (0, 6), timeout=20)
        assert len(running) == 2

        stopping = asyncio.create_task(broker.stop())
        await await_counts(rabbit, queue, (4, 2), timeout=10)
        assert not stopping.done()
        assert len(running) == 2
    finally:
        release.set()
        if stopping is not None:
            await asyncio.wait_for(stopping, timeout=40)
        else:
            await broker.stop(timeout=2)
    await await_counts(rabbit, queue, (4, 0), timeout=20)
    assert len(running) == 2
