"""Real-broker coverage for the producer outbox (``rabbitkit.outbox``).

* a relay process killed with SIGKILL mid-flight, then a new relay on the same
  file: every message delivered at least once, none lost, duplicates bounded
  by ``max_in_flight`` and carrying the original ``message_id`` (async)
* the same with a sync relay that dies mid-pass, and the documented
  dedicated-relay-thread recipe for ``SyncBroker``
* broker unavailable (``rabbitmqctl stop_app``): entries stay pending and are
  delivered once the broker is back (async)
* an unroutable ``mandatory`` message ends in the failed state, not lost, and
  is delivered after a replay once its queue exists (async and sync)

The outage test uses this module's own broker (it stops the node's app).
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import AsyncIterator, Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.integration.conftest import QueueProbe, skip_without_docker

pytestmark = pytest.mark.integration


@pytest.fixture
def rabbit(rabbit_container: dict[str, Any]) -> dict[str, Any]:
    return rabbit_container


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


def _name(prefix: str) -> str:
    return f"rk-outbox-{prefix}-{uuid.uuid4().hex[:8]}"


def _declare(url: str, queue: str) -> None:
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(url))
    try:
        conn.channel().queue_declare(queue, durable=True)
    finally:
        conn.close()


def _drain_queue(url: str, queue: str) -> list[tuple[bytes, str]]:
    """Every message in *queue* as ``(body, message_id)`` (consumed)."""
    import pika

    conn = pika.BlockingConnection(pika.URLParameters(url))
    got: list[tuple[bytes, str]] = []
    try:
        channel = conn.channel()
        while True:
            method, properties, body = channel.basic_get(queue, auto_ack=True)
            if method is None:
                return got
            got.append((body, properties.message_id))
    finally:
        conn.close()


def _envelopes(queue: str, count: int) -> list[Any]:
    from rabbitkit import MessageEnvelope

    return [MessageEnvelope(routing_key=queue, body=f"m{n}".encode(), mandatory=True) for n in range(count)]


def _assert_at_least_once(got: list[tuple[bytes, str]], count: int, max_duplicates: int) -> int:
    bodies = [body for body, _mid in got]
    assert set(bodies) == {f"m{n}".encode() for n in range(count)}, "a message was lost"
    ids: dict[bytes, set[str]] = {}
    for body, message_id in got:
        ids.setdefault(body, set()).add(message_id)
    assert all(len(v) == 1 for v in ids.values()), "a republish changed the message_id"
    duplicates = len(got) - count
    assert 0 <= duplicates <= max_duplicates, f"{duplicates} duplicates, more than the in-flight window"
    return duplicates


def _async_config(url: str, **kw: Any) -> Any:
    import dataclasses

    from rabbitkit import ConnectionConfig, RabbitConfig

    connection = dataclasses.replace(
        ConnectionConfig.from_url(url), reconnect_backoff_base=0.2, reconnect_backoff_max=1.0
    )
    return RabbitConfig(connection=connection, **kw)


@contextlib.asynccontextmanager
async def _started(broker: Any) -> AsyncIterator[Any]:
    await broker.start(install_signal_handlers=False)
    try:
        yield broker
    finally:
        async with asyncio.timeout(30):
            await broker.stop()


async def _until(predicate: Any, timeout: float, what: str) -> None:
    deadline = time.monotonic() + timeout
    while not await predicate():
        assert time.monotonic() < deadline, f"timed out waiting for {what}"
        await asyncio.sleep(0.05)


def _fast(**kw: Any) -> Any:
    from rabbitkit import OutboxConfig

    kw.setdefault("backoff_initial", 0.05)
    kw.setdefault("backoff_max", 0.5)
    kw.setdefault("poll_interval", 0.05)
    return OutboxConfig(**kw)


# ── crash and restart ─────────────────────────────────────────────────────

_CHILD = """
import asyncio
import sys

from rabbitkit import AsyncBroker, AsyncOutbox, ConnectionConfig, OutboxConfig, RabbitConfig


class Throttled:
    # Slows each publish so the parent can kill the relay mid-flight.
    def __init__(self, broker):
        self.broker = broker
        self.config = broker.config

    async def publish(self, envelope=None, *, timeout=None):
        await asyncio.sleep(0.01)
        return await self.broker.publish(envelope)


async def main():
    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(sys.argv[1])))
    await broker.start(install_signal_handlers=False)
    outbox = AsyncOutbox(Throttled(broker), sys.argv[2], OutboxConfig(max_in_flight=int(sys.argv[3])))
    await outbox.start()
    print("relaying", flush=True)
    await asyncio.Event().wait()


asyncio.run(main())
"""


async def test_async_relay_killed_mid_flight_then_restarted_loses_nothing(
    rabbit: dict[str, Any], tmp_path: Path
) -> None:
    from rabbitkit import AsyncBroker, AsyncOutbox, OutboxStore

    queue = _name("crash")
    total, window = 1500, 20
    db = tmp_path / "outbox.db"
    await asyncio.to_thread(_declare, rabbit["url"], queue)
    store = OutboxStore(db)
    await asyncio.to_thread(store.enqueue_many, _envelopes(queue, total))
    store.close()

    script = tmp_path / "relay_child.py"
    script.write_text(_CHILD)
    child = subprocess.Popen(  # noqa: S603 -- our own script, our interpreter
        [sys.executable, str(script), rabbit["url"], str(db), str(window)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=tmp_path,
    )
    try:
        with QueueProbe(rabbit["url"]) as probe:
            deadline = time.monotonic() + 60
            while (probe.ready(queue) or 0) < total // 4:
                assert child.poll() is None, child.stderr.read().decode() if child.stderr else "child exited"
                assert time.monotonic() < deadline, "the child relay never got going"
                await asyncio.sleep(0.01)
        os.kill(child.pid, signal.SIGKILL)  # crash: no stop(), no close, no final transaction
        child.wait(timeout=10)
    finally:
        if child.poll() is None:
            child.kill()
            child.wait(timeout=10)
    assert child.returncode == -signal.SIGKILL

    survivors = OutboxStore(db)
    left = await asyncio.to_thread(survivors.stats)
    survivors.close()
    assert 0 < left.pending < total, "the kill did not land mid-flight"

    broker = AsyncBroker(_async_config(rabbit["url"]))
    async with asyncio.timeout(120), _started(broker):
        outbox = AsyncOutbox(broker, db, _fast(max_in_flight=window))
        await outbox.start()

        async def drained() -> bool:
            return (await outbox.stats()).pending == 0

        await _until(drained, 60, "the restarted relay to drain the file")
        assert await outbox.stop() is True

    got = await asyncio.to_thread(_drain_queue, rabbit["url"], queue)
    _assert_at_least_once(got, total, max_duplicates=window)


def test_sync_relay_dying_mid_pass_then_restarted_loses_nothing(rabbit: dict[str, Any], tmp_path: Path) -> None:
    from rabbitkit import ConnectionConfig, RabbitConfig, SyncBroker, SyncOutbox

    queue = _name("sync-crash")
    total, window = 100, 10
    db = tmp_path / "outbox.db"
    _declare(rabbit["url"], queue)

    class DiesAfter:
        """Publishes for real, then 'crashes' (a BaseException the relay does
        not catch) right after the 25th confirm: its pass is never recorded."""

        def __init__(self, broker: Any, after: int) -> None:
            self.broker, self.after, self.count = broker, after, 0
            self.config = broker.config

        def publish(self, envelope: Any = None) -> Any:
            self.count += 1
            outcome = self.broker.publish(envelope)
            if self.count == self.after:
                raise SystemExit("process died")
            return outcome

    config = RabbitConfig(connection=ConnectionConfig.from_url(rabbit["url"]))
    first = SyncBroker(config)
    first.start()
    try:
        crashed = SyncOutbox(DiesAfter(first, after=25), db, _fast(max_in_flight=window))
        crashed.enqueue_many(_envelopes(queue, total))
        with pytest.raises(SystemExit):
            crashed.drain(timeout=30)
        # abandoned: no close()
        second = SyncBroker(config)
        second.start()
        try:
            restarted = SyncOutbox(second, db, _fast(max_in_flight=window))
            assert restarted.drain(timeout=60) is True
            assert restarted.stats().pending == 0
            restarted.close()
        finally:
            second.stop()
    finally:
        first.stop()

    got = _drain_queue(rabbit["url"], queue)
    duplicates = _assert_at_least_once(got, total, max_duplicates=window)
    assert duplicates == 25 - 20  # the 5 confirmed in the pass that never got recorded


def test_sync_relay_on_its_own_thread_with_its_own_broker(rabbit: dict[str, Any], tmp_path: Path) -> None:
    """The documented recipe: a publish-only SyncBroker started on the relay
    thread, application threads enqueue."""
    from rabbitkit import ConnectionConfig, RabbitConfig, SyncBroker, SyncOutbox

    queue = _name("sync-thread")
    _declare(rabbit["url"], queue)
    broker = SyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(rabbit["url"])))
    outbox = SyncOutbox(broker, tmp_path / "outbox.db", _fast(poll_interval=5.0))
    drained: list[bool] = []

    def relay() -> None:
        broker.start()  # this thread owns the connection
        try:
            drained.append(outbox.run())
        finally:
            broker.stop()

    thread = threading.Thread(target=relay, name="outbox-relay")
    thread.start()
    try:
        for envelope in _envelopes(queue, 50):
            outbox.enqueue(envelope)  # any thread
        with QueueProbe(rabbit["url"]) as probe:
            deadline = time.monotonic() + 30
            while (probe.ready(queue) or 0) < 50:
                assert time.monotonic() < deadline, "the relay thread did not publish"
                time.sleep(0.02)
    finally:
        outbox.request_stop()
        thread.join(timeout=30)
    assert drained == [True]
    assert outbox.stats().pending == 0
    _assert_at_least_once(_drain_queue(rabbit["url"], queue), 50, max_duplicates=0)


# ── broker unavailable ────────────────────────────────────────────────────


async def test_entries_stay_pending_while_the_broker_is_down_and_are_delivered_after(
    own_rabbit: dict[str, Any], tmp_path: Path
) -> None:
    from rabbitkit import AsyncBroker, AsyncOutbox, MetricsMiddleware, OutboxEntryState
    from tests.integration.test_observability_foundation import History

    queue = _name("outage")
    await asyncio.to_thread(_declare, own_rabbit["url"], queue)
    history = History()
    broker = AsyncBroker(_async_config(own_rabbit["url"]), middlewares=[MetricsMiddleware(history)])
    async with asyncio.timeout(180), _started(broker):
        outbox = AsyncOutbox(broker, tmp_path / "outbox.db", _fast(publish_timeout=2.0))
        await outbox.start()
        await asyncio.to_thread(_ctl, own_rabbit, "stop_app")
        try:
            await outbox.enqueue_many(_envelopes(queue, 20))

            async def retried() -> bool:
                entries = await outbox.list_entries(OutboxEntryState.PENDING)
                return bool(entries) and all(e.attempts >= 1 for e in entries[:1])

            await _until(retried, 60, "the relay to try while the broker is down")
            stats = await outbox.stats()
            assert (stats.pending, stats.failed) == (20, 0)
            assert history.count("rabbitkit_outbox_publish_retries_total") >= 1
            assert history.count("rabbitkit_outbox_failed_total") == 0
        finally:
            await asyncio.to_thread(_ctl, own_rabbit, "start_app")
            await asyncio.to_thread(_ctl, own_rabbit, "await_startup")

        async def delivered() -> bool:
            return (await outbox.stats()).pending == 0

        await _until(delivered, 90, "delivery after the broker came back")
        assert await outbox.stop() is True
        assert history.count("rabbitkit_outbox_relayed_total") == 20
        assert history.gauge("rabbitkit_outbox_pending") == 0.0

    got = await asyncio.to_thread(_drain_queue, own_rabbit["url"], queue)
    # an UNKNOWN/TIMEOUT attempt that did reach the broker is republished: bounded by the batch
    _assert_at_least_once(got, 20, max_duplicates=20)


# ── unroutable mandatory ──────────────────────────────────────────────────


async def test_unroutable_mandatory_message_is_failed_not_lost_and_replayable(
    rabbit: dict[str, Any], tmp_path: Path
) -> None:
    from rabbitkit import AsyncBroker, AsyncOutbox, MessageEnvelope, MetricsMiddleware
    from tests.integration.test_observability_foundation import History

    queue = _name("unroutable")
    history = History()
    broker = AsyncBroker(_async_config(rabbit["url"]), middlewares=[MetricsMiddleware(history)])
    async with asyncio.timeout(60), _started(broker):
        outbox = AsyncOutbox(broker, tmp_path / "outbox.db", _fast(max_refusals=2))
        await outbox.start()
        await outbox.enqueue(MessageEnvelope(routing_key=queue, body=b"m0", mandatory=True, message_id="order-1"))

        async def failed() -> bool:
            return (await outbox.stats()).failed == 1

        await _until(failed, 30, "the unroutable entry to fail")
        [entry] = await outbox.list_entries()
        assert (entry.last_status, entry.last_reason, entry.refusals) == ("returned", "returned", 2)
        assert entry.envelope.message_id == "order-1"
        assert history.count("rabbitkit_outbox_failed_total", status="returned", reason="returned") == 1

        await asyncio.to_thread(_declare, rabbit["url"], queue)
        assert await outbox.replay_failed() == 1

        async def empty() -> bool:
            return (await outbox.stats()).entries == 0

        await _until(empty, 30, "the replayed entry to be delivered")
        await outbox.stop()
    assert await asyncio.to_thread(_drain_queue, rabbit["url"], queue) == [(b"m0", "order-1")]


def test_sync_unroutable_mandatory_message_is_failed_not_lost(rabbit: dict[str, Any], tmp_path: Path) -> None:
    from rabbitkit import ConnectionConfig, MessageEnvelope, RabbitConfig, SyncBroker, SyncOutbox

    broker = SyncBroker(RabbitConfig(connection=ConnectionConfig.from_url(rabbit["url"])))
    broker.start()
    try:
        outbox = SyncOutbox(broker, tmp_path / "outbox.db", _fast(max_refusals=1))
        outbox.enqueue(MessageEnvelope(routing_key=_name("nowhere"), body=b"x", mandatory=True))
        report = outbox.relay_once()
        assert (report.attempted, report.failed) == (1, 1)
        [entry] = outbox.list_entries()
        assert entry.last_status == "returned"
        assert outbox.stats().pending == 0
        outbox.close()
    finally:
        broker.stop()
