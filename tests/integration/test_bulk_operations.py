"""Real-RabbitMQ integration tests for bulk operations (plan §7/§8 acceptance).

Requires Docker + testcontainers (auto-skipped otherwise), same as
``test_real_rabbitmq.py``. Run with::

    pytest tests/integration/test_bulk_operations.py -m integration -v

Covers, against a live broker:

* ``publish_many`` (sync + async): every input accounted for, CONFIRMED items
  actually consumable, an unroutable item reported UNROUTABLE (not
  confirmed), an oversized item rejected before submission.
* ``ack_many`` (sync + async) with MANUAL handlers: acking a SUBSET leaves
  the unselected sibling unacked (visible as ``messages_unacknowledged``
  on the broker), then acking the rest drains the queue.
* ``nack_many(requeue=False)`` dead-letters into the auto-provisioned DLQ.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

import pytest

try:
    from rabbitkit.core.message import RabbitMessage as _RabbitMessage  # noqa: F401
except ImportError:  # pragma: no cover
    pass

try:
    from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

    _TESTCONTAINERS_AVAILABLE = True
except ImportError:
    _TESTCONTAINERS_AVAILABLE = False

pytestmark = pytest.mark.integration


def _skip_no_docker() -> None:
    if not _TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not installed — run: pip install testcontainers[rabbitmq]")
    try:
        import docker  # type: ignore[import-untyped]

        docker.from_env().ping()
    except Exception:
        pytest.skip("Docker daemon not reachable — skip real-RabbitMQ integration tests")


@pytest.fixture(scope="module")
def rabbit() -> Any:  # type: ignore[return]
    _skip_no_docker()
    with RabbitMqContainer("rabbitmq:3.13-management-alpine").with_exposed_ports(15672) as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5672)
        mgmt_port = container.get_exposed_port(15672)
        yield {
            "url": f"amqp://guest:guest@{host}:{port}/",
            "mgmt": f"http://{host}:{mgmt_port}",
        }


def _config(url: str, **kw: Any) -> Any:
    from rabbitkit.core.config import ConnectionConfig, PublisherConfig, RabbitConfig

    kw.setdefault("publisher", PublisherConfig(mandatory=True, max_message_bytes=64 * 1024))
    return RabbitConfig(connection=ConnectionConfig.from_url(url), **kw)


def _mgmt(rabbit: dict[str, str]) -> Any:
    from rabbitkit.management import ManagementConfig, RabbitManagementClient

    return RabbitManagementClient(ManagementConfig(url=rabbit["mgmt"], username="guest", password="guest"))


def _queue_counts(rabbit: dict[str, str], queue: str, *, retries: int = 40) -> tuple[int, int]:
    """``(ready, unacked)`` from the management API; polls until the queue exists."""
    client = _mgmt(rabbit)
    last: Exception | None = None
    for _ in range(retries):
        try:
            info = client.get_queue(queue)
            return int(info.get("messages_ready", 0)), int(info.get("messages_unacknowledged", 0))
        except Exception as exc:  # 404 until declared / stats lag
            last = exc
            time.sleep(0.25)
    raise AssertionError(f"queue {queue} not visible via management API: {last}")


def _wait_until(pred: Any, timeout: float = 15.0) -> None:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        time.sleep(0.1)
    raise AssertionError("condition not met in time")


def _envs(queue: str, n: int) -> list[Any]:
    from rabbitkit.core.types import MessageEnvelope

    return [
        MessageEnvelope(routing_key=queue, body=f'{{"i": {i}}}'.encode(), message_id=f"{queue}-{i}", mandatory=True)
        for i in range(n)
    ]


# ══════════════════════════════════════════════════════════════════════════════
# Async
# ══════════════════════════════════════════════════════════════════════════════


async def test_async_publish_many_confirmed_and_consumed(rabbit: dict[str, str]) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.bulk import BulkPublishOptions
    from rabbitkit.core.types import BulkPublishStatus, MessageEnvelope

    queue = f"bulk-async-{uuid.uuid4().hex[:8]}"
    received: list[bytes] = []
    done = asyncio.Event()
    broker = AsyncBroker(config=_config(rabbit["url"]))

    @broker.subscriber(queue=queue)
    async def handle(body: bytes) -> None:
        received.append(body)
        if len(received) >= 50:
            done.set()

    await broker.start()
    await asyncio.sleep(0.3)

    envelopes = _envs(queue, 50)
    # one unroutable (no such queue, mandatory) + one oversized
    envelopes.append(MessageEnvelope(routing_key=f"{queue}-missing", body=b"{}", mandatory=True))
    envelopes.append(MessageEnvelope(routing_key=queue, body=b"x" * (65 * 1024)))

    result = await broker.publish_many(envelopes, BulkPublishOptions(max_in_flight=16, overall_timeout=60))
    assert len(result) == 52
    assert result.counts[BulkPublishStatus.CONFIRMED] == 50
    assert result.items[50].status is BulkPublishStatus.UNROUTABLE
    assert result.items[51].status is BulkPublishStatus.INVALID
    assert all(it.attempt_id for it in result.items[:50])

    await asyncio.wait_for(done.wait(), timeout=30.0)
    assert len(received) == 50
    await broker.stop()


async def test_async_ack_many_subset_leaves_sibling_unacked(rabbit: dict[str, str]) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConsumerConfig
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.types import AckPolicy

    queue = f"bulk-ack-async-{uuid.uuid4().hex[:8]}"
    held: list[RabbitMessage] = []
    got3 = asyncio.Event()
    broker = AsyncBroker(config=_config(rabbit["url"], consumer=ConsumerConfig(prefetch_count=10)))

    @broker.subscriber(queue=queue, ack_policy=AckPolicy.MANUAL)
    async def handle(body: bytes, msg: RabbitMessage) -> None:
        held.append(msg)
        if len(held) >= 3:
            got3.set()

    await broker.start()
    await asyncio.sleep(0.3)
    result = await broker.publish_many(_envs(queue, 3))
    assert result.all_confirmed
    await asyncio.wait_for(got3.wait(), timeout=30.0)

    held.sort(key=lambda m: m.delivery_tag or 0)
    report = await broker.ack_many([held[0], held[2]])
    assert report.all_dispatched
    assert not held[1].is_settled

    # Broker view: exactly one delivery still unacknowledged.
    await asyncio.get_running_loop().run_in_executor(
        None, lambda: _wait_until(lambda: _queue_counts(rabbit, queue) == (0, 1))
    )

    report = await broker.ack_many([held[1]])
    assert report.all_dispatched
    await asyncio.get_running_loop().run_in_executor(
        None, lambda: _wait_until(lambda: _queue_counts(rabbit, queue) == (0, 0))
    )
    await broker.stop()


async def test_async_nack_many_dead_letters(rabbit: dict[str, str]) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.types import AckPolicy

    queue = f"bulk-nack-async-{uuid.uuid4().hex[:8]}"
    held: list[RabbitMessage] = []
    got = asyncio.Event()
    broker = AsyncBroker(config=_config(rabbit["url"]))

    @broker.subscriber(queue=queue, ack_policy=AckPolicy.MANUAL)
    async def handle(body: bytes, msg: RabbitMessage) -> None:
        held.append(msg)
        if len(held) >= 2:
            got.set()

    await broker.start()
    await asyncio.sleep(0.3)
    assert (await broker.publish_many(_envs(queue, 2))).all_confirmed
    await asyncio.wait_for(got.wait(), timeout=30.0)

    report = await broker.nack_many(held, requeue=False)
    assert report.all_dispatched
    await asyncio.get_running_loop().run_in_executor(
        None, lambda: _wait_until(lambda: _queue_counts(rabbit, f"{queue}.dlq")[0] == 2)
    )
    await broker.stop()


# ══════════════════════════════════════════════════════════════════════════════
# Sync
# ══════════════════════════════════════════════════════════════════════════════


def _pump_until(broker: Any, pred: Any, timeout: float = 30.0) -> None:
    """Drive the sync broker's I/O loop FROM THE OWNER THREAD until *pred*.

    The sync transport is owner-thread-bound (see docs/concurrency-model.md):
    pumping from a helper thread while the test thread later calls
    ``broker.stop()`` is exactly the cross-thread use the library forbids.
    """
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if pred():
            return
        broker.pump_idle(0.05)
    raise AssertionError("condition not met while pumping")


def test_sync_publish_many_confirmed_and_consumed(rabbit: dict[str, str]) -> None:
    from rabbitkit.core.types import BulkPublishStatus, MessageEnvelope
    from rabbitkit.sync.broker import SyncBroker

    queue = f"bulk-sync-{uuid.uuid4().hex[:8]}"
    received: list[bytes] = []
    broker = SyncBroker(config=_config(rabbit["url"]))

    @broker.subscriber(queue=queue)
    def handle(body: bytes) -> None:
        received.append(body)

    broker.start()
    try:
        envelopes = _envs(queue, 20)
        envelopes.append(MessageEnvelope(routing_key=f"{queue}-missing", body=b"{}", mandatory=True))
        result = broker.publish_many(envelopes)
        assert len(result) == 21
        assert result.counts[BulkPublishStatus.CONFIRMED] == 20
        assert result.items[20].status is BulkPublishStatus.UNROUTABLE

        _pump_until(broker, lambda: len(received) == 20)
    finally:
        broker.stop()


def test_sync_ack_many_subset(rabbit: dict[str, str]) -> None:
    from rabbitkit.core.config import ConsumerConfig
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.types import AckPolicy
    from rabbitkit.sync.broker import SyncBroker

    queue = f"bulk-ack-sync-{uuid.uuid4().hex[:8]}"
    held: list[RabbitMessage] = []
    broker = SyncBroker(config=_config(rabbit["url"], consumer=ConsumerConfig(prefetch_count=10)))

    @broker.subscriber(queue=queue, ack_policy=AckPolicy.MANUAL)
    def handle(body: bytes, msg: RabbitMessage) -> None:
        held.append(msg)

    broker.start()
    try:
        assert broker.publish_many(_envs(queue, 3)).all_confirmed
        _pump_until(broker, lambda: len(held) >= 3)
        assert len(held) == 3
        held.sort(key=lambda m: m.delivery_tag or 0)

        # Settle from the owner thread (sync brokers are owner-thread-bound).
        report = broker.ack_many([held[0], held[2]])
        assert report.all_dispatched
        assert not held[1].is_settled
        for _ in range(10):
            broker.pump_idle(0.05)
        _wait_until(lambda: _queue_counts(rabbit, queue) == (0, 1))

        assert broker.nack_many([held[1]], requeue=False).all_dispatched
        for _ in range(10):
            broker.pump_idle(0.05)
        _wait_until(lambda: _queue_counts(rabbit, queue) == (0, 0))
        _wait_until(lambda: _queue_counts(rabbit, f"{queue}.dlq")[0] == 1)
    finally:
        broker.stop()


def test_preflight_against_live_management_api(rabbit: dict[str, str]) -> None:
    """Critical-profile preflight with a real management client: a quorum
    queue without the at-least-once policy is FAILED, not silently green."""
    from rabbitkit.core.profiles import critical_config
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.core.types import PreflightStatus, QueueType
    from rabbitkit.sync.broker import SyncBroker

    queue = f"bulk-preflight-{uuid.uuid4().hex[:8]}"
    broker = SyncBroker(config=critical_config(_config(rabbit["url"])))

    @broker.subscriber(queue=RabbitQueue(name=queue, queue_type=QueueType.QUORUM))
    def handle(body: bytes) -> None:
        pass

    broker.start()
    try:
        _queue_counts(rabbit, queue)  # wait until declared
        report = broker.preflight("critical", management_client=_mgmt(rabbit))
        by_name = {c.name: c.status for c in report.checks}
        assert by_name[f"broker:{queue}:type"] is PreflightStatus.VERIFIED
        assert by_name[f"broker:{queue}:dead-letter-strategy"] is PreflightStatus.FAILED
        assert not report.ok
    finally:
        broker.stop()
