"""Real-RabbitMQ integration tests for the 0.12 reliability features.

Companion to ``test_bulk_operations.py`` (same Docker/testcontainers skip
guard). Covers, against a live broker:

* streaming ``iter_publish`` over an async generator (bounded in-flight);
* ``publish_many`` with confirms OFF → every item ``UNKNOWN`` / ``sent_unconfirmed``;
* ``RetryConfig(delay_queue_type="quorum")`` declares quorum delay queues (management API);
* sanitized triage headers on the actual retry envelope (peeked via ``basic_get``);
* retry-handoff failure: delay queue deleted → mandatory retry publish RETURNED →
  source nack-requeued, tracker counts the failure, redelivery succeeds;
* policy templates applied through ``put_policy`` → critical preflight fully VERIFIED;
* ``CoalescingAcker`` with out-of-order completion against a real channel.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from collections.abc import AsyncIterator
from typing import Any

import pytest

try:
    from rabbitkit.core.message import RabbitMessage as _RabbitMessage
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
        yield {
            "url": f"amqp://guest:guest@{host}:{container.get_exposed_port(5672)}/",
            "mgmt": f"http://{host}:{container.get_exposed_port(15672)}",
        }


def _config(url: str, **kw: Any) -> Any:
    from rabbitkit.core.config import ConnectionConfig, PublisherConfig, RabbitConfig

    kw.setdefault("publisher", PublisherConfig(mandatory=True, max_message_bytes=64 * 1024))
    return RabbitConfig(connection=ConnectionConfig.from_url(url), **kw)


def _mgmt(rabbit: dict[str, str]) -> Any:
    from rabbitkit.management import ManagementConfig, RabbitManagementClient

    return RabbitManagementClient(ManagementConfig(url=rabbit["mgmt"], username="guest", password="guest"))


def _queue_info(rabbit: dict[str, str], queue: str, *, retries: int = 40) -> dict[str, Any]:
    client = _mgmt(rabbit)
    last: Exception | None = None
    for _ in range(retries):
        try:
            return dict(client.get_queue(queue))
        except Exception as exc:
            last = exc
            time.sleep(0.25)
    raise AssertionError(f"queue {queue} not visible via management API: {last}")


def _counts(rabbit: dict[str, str], queue: str) -> tuple[int, int]:
    info = _queue_info(rabbit, queue)
    return int(info.get("messages_ready", 0)), int(info.get("messages_unacknowledged", 0))


async def _await_until(pred: Any, timeout: float = 20.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while loop.time() < deadline:
        if await loop.run_in_executor(None, pred):
            return
        await asyncio.sleep(0.2)
    raise AssertionError("condition not met in time")


# ── iter_publish streaming ──────────────────────────────────────────────────


async def test_async_iter_publish_streams_a_generator(rabbit: dict[str, str]) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.bulk import BulkPublishOptions
    from rabbitkit.core.types import MessageEnvelope

    queue = f"rel-stream-{uuid.uuid4().hex[:8]}"
    received: list[bytes] = []
    done = asyncio.Event()
    broker = AsyncBroker(config=_config(rabbit["url"]))

    @broker.subscriber(queue=queue)
    async def handle(body: bytes) -> None:
        received.append(body)
        if len(received) >= 200:
            done.set()

    async def rows() -> AsyncIterator[MessageEnvelope]:
        for i in range(200):
            yield MessageEnvelope(
                routing_key=queue, body=f'{{"i":{i}}}'.encode(), message_id=f"{queue}-{i}", mandatory=True
            )

    await broker.start()
    await asyncio.sleep(0.3)
    seen: list[int] = []
    async for item in broker.iter_publish(rows(), BulkPublishOptions(max_in_flight=8, overall_timeout=60)):
        assert item.ok, (item.status, item.reason)
        seen.append(item.index)
    assert sorted(seen) == list(range(200))
    await asyncio.wait_for(done.wait(), timeout=30)
    await broker.stop()


# ── confirms off → UNKNOWN, never "confirmed" ───────────────────────────────


async def test_publish_many_without_confirms_reports_unknown(rabbit: dict[str, str]) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.bulk import REASON_SENT_UNCONFIRMED
    from rabbitkit.core.config import PublisherConfig
    from rabbitkit.core.types import BulkPublishStatus, MessageEnvelope

    queue = f"rel-noconfirm-{uuid.uuid4().hex[:8]}"
    publisher = PublisherConfig(confirm_delivery=False, mandatory=False)
    broker = AsyncBroker(config=_config(rabbit["url"], publisher=publisher))

    @broker.subscriber(queue=queue)
    async def handle(body: bytes) -> None:
        pass

    await broker.start()
    await asyncio.sleep(0.3)
    result = await broker.publish_many(
        [MessageEnvelope(routing_key=queue, body=b"{}", mandatory=False) for _ in range(5)]
    )
    assert all(it.status is BulkPublishStatus.UNKNOWN and it.reason == REASON_SENT_UNCONFIRMED for it in result.items)
    assert not result.all_confirmed
    await broker.stop()


# ── quorum delay chain ──────────────────────────────────────────────────────


async def test_quorum_delay_queue_type_is_declared(rabbit: dict[str, str]) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import RetryConfig
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.core.types import QueueType

    queue = f"rel-quorum-{uuid.uuid4().hex[:8]}"
    retry = RetryConfig(max_retries=2, delays=(60, 120), delay_queue_type="quorum", dlq_queue_type="quorum")
    broker = AsyncBroker(config=_config(rabbit["url"], retry=retry))

    @broker.subscriber(queue=RabbitQueue(name=queue, queue_type=QueueType.QUORUM))
    async def handle(body: bytes) -> None:
        pass

    await broker.start()
    try:
        loop = asyncio.get_running_loop()
        for name in (queue, f"{queue}.retry.1", f"{queue}.retry.2", f"{queue}.dlq"):
            info = await loop.run_in_executor(None, lambda n=name: _queue_info(rabbit, n))
            assert info.get("type") == "quorum", (name, info.get("type"))
    finally:
        await broker.stop()


# ── sanitized headers on the real retry envelope ────────────────────────────


async def test_retry_envelope_headers_are_sanitized_on_the_wire(rabbit: dict[str, str]) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import RetryConfig
    from rabbitkit.core.types import MessageEnvelope

    secret = "s3cr3tP@ss"
    queue = f"rel-sanit-{uuid.uuid4().hex[:8]}"
    broker = AsyncBroker(config=_config(rabbit["url"], retry=RetryConfig(max_retries=1, delays=(60,))))
    failed = asyncio.Event()

    @broker.subscriber(queue=queue)
    async def handle(body: bytes) -> None:
        failed.set()
        raise ConnectionError(f"amqp://svc:{secret}@db.internal/ refused; token={secret}")  # transient

    await broker.start()
    await asyncio.sleep(0.3)
    (await broker.publish_many([MessageEnvelope(routing_key=queue, body=b"{}", mandatory=True)])).raise_for_status()
    await asyncio.wait_for(failed.wait(), timeout=30)

    # The retry envelope now sits in <queue>.retry.1 for 60s — peek at it.
    await _await_until(lambda: _counts(rabbit, f"{queue}.retry.1")[0] == 1)
    peeked = await broker._transport.basic_get(f"{queue}.retry.1")
    assert peeked is not None
    headers = peeked.headers
    assert secret not in repr(headers)
    assert headers["x-rabbitkit-error-category"] == "transient"
    assert headers["x-rabbitkit-error-type"] == "ConnectionError"
    assert "***" in headers["x-rabbitkit-error-message"]
    assert headers["x-rabbitkit-retry-count"] == 1
    await peeked.ack_async()
    await broker.stop()


# ── retry handoff failure against a deleted delay queue ─────────────────────


async def test_retry_handoff_failure_nacks_and_recovers(rabbit: dict[str, str]) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import RetryConfig, RetryHandoffConfig
    from rabbitkit.core.types import HandoffState, MessageEnvelope
    from rabbitkit.middleware.retry import RetryMiddleware

    queue = f"rel-handoff-{uuid.uuid4().hex[:8]}"
    retry = RetryConfig(
        max_retries=1, delays=(60,), handoff=RetryHandoffConfig(backoff_initial=0.2, backoff_max=0.5, jitter=0.0)
    )
    broker = AsyncBroker(config=_config(rabbit["url"], retry=retry))
    attempts: list[bool] = []
    recovered = asyncio.Event()

    @broker.subscriber(queue=queue)
    async def handle(body: bytes, msg: _RabbitMessage) -> None:
        attempts.append(msg.redelivered)
        if len(attempts) == 1:
            raise ConnectionError("downstream down")  # transient → retry publish → RETURNED (queue deleted)
        recovered.set()

    await broker.start()
    await asyncio.sleep(0.3)
    loop = asyncio.get_running_loop()
    await loop.run_in_executor(None, lambda: _mgmt(rabbit).delete_queue(f"{queue}.retry.1"))

    (await broker.publish_many([MessageEnvelope(routing_key=queue, body=b"{}", mandatory=True)])).raise_for_status()
    await asyncio.wait_for(recovered.wait(), timeout=30)

    route = next(r for r in broker.routes if r.queue.name == queue)
    mw = next(m for m in route.route_middlewares if isinstance(m, RetryMiddleware))
    assert mw.handoff_tracker.total_failures >= 1
    assert attempts[0] is False and attempts[1] is True  # nack-requeued → redelivered, never acked early
    assert mw.handoff_tracker.state in (HandoffState.DEGRADED, HandoffState.HEALTHY)
    await broker.stop()


# ── policy templates → preflight fully verified ─────────────────────────────


def test_policy_templates_applied_make_critical_preflight_fully_verified(rabbit: dict[str, str]) -> None:
    from rabbitkit.core.profiles import critical_config, policy_templates
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.core.types import PreflightStatus, QueueType
    from rabbitkit.sync.broker import SyncBroker

    queue = f"rel-policy-{uuid.uuid4().hex[:8]}"
    config = critical_config(_config(rabbit["url"]))
    assert config.retry is not None
    mgmt = _mgmt(rabbit)
    templates = policy_templates([queue], retry=config.retry, profile="critical")
    for tpl in templates:
        mgmt.put_policy(tpl.name, tpl)
    try:
        broker = SyncBroker(config=config)

        @broker.subscriber(queue=RabbitQueue(name=queue, queue_type=QueueType.QUORUM))
        def handle(body: bytes) -> None:
            pass

        broker.start()
        try:
            deadline = time.monotonic() + 20
            while time.monotonic() < deadline:
                info = _queue_info(rabbit, queue)
                if (info.get("effective_policy_definition") or {}).get("dead-letter-strategy") == "at-least-once":
                    break
                time.sleep(0.25)
            report = broker.preflight("critical", management_client=mgmt)
            assert report.fully_verified, [(c.name, c.status.value, c.detail) for c in report.checks]
            assert all(c.status is PreflightStatus.VERIFIED for c in report.checks)
            assert {p["name"] for p in mgmt.list_policies("/")} >= {t.name for t in templates}
        finally:
            broker.stop()
    finally:
        for tpl in templates:
            mgmt.delete_policy(tpl.name)


# ── CoalescingAcker on a real channel ───────────────────────────────────────


async def test_coalescing_acker_settles_out_of_order_completions(rabbit: dict[str, str]) -> None:
    import random

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import BatchAckConfig, ConsumerConfig, WorkerConfig
    from rabbitkit.core.types import AckPolicy, MessageEnvelope
    from rabbitkit.highload.batch import CoalescingAcker

    queue = f"rel-coalesce-{uuid.uuid4().hex[:8]}"
    total = 30
    broker = AsyncBroker(config=_config(rabbit["url"], consumer=ConsumerConfig(prefetch_count=64)))
    loop = asyncio.get_running_loop()
    channel: dict[str, Any] = {}
    frames: list[tuple[int, bool]] = []
    completed = 0
    done = asyncio.Event()

    def ack_fn(tag: int, multiple: bool) -> None:
        frames.append((tag, multiple))
        loop.create_task(channel["ch"].basic_ack(delivery_tag=tag, multiple=multiple))

    def nack_fn(tag: int, requeue: bool) -> None:
        loop.create_task(channel["ch"].basic_nack(delivery_tag=tag, requeue=requeue))

    def reject_fn(tag: int, requeue: bool) -> None:
        loop.create_task(channel["ch"].basic_reject(delivery_tag=tag, requeue=requeue))

    # flush_interval_ms=0 → we flush manually from the event loop (ack_fn creates tasks)
    acker = CoalescingAcker(
        ack_fn=ack_fn, nack_fn=nack_fn, reject_fn=reject_fn, config=BatchAckConfig(batch_size=1000, flush_interval_ms=0)
    )

    @broker.subscriber(queue=queue, ack_policy=AckPolicy.MANUAL)
    async def handle(body: bytes, msg: _RabbitMessage) -> None:
        nonlocal completed
        assert msg.delivery_tag is not None
        channel.setdefault("ch", msg.raw_message.channel)
        acker.register(msg.delivery_tag)
        await asyncio.sleep(random.uniform(0, 0.15))
        acker.complete(msg.delivery_tag)
        completed += 1
        if completed >= total:
            done.set()

    await broker.start(worker_config=WorkerConfig(worker_count=4))
    await asyncio.sleep(0.3)
    envelopes = [MessageEnvelope(routing_key=queue, body=b"{}") for _ in range(total)]
    (await broker.publish_many(envelopes)).raise_for_status()
    await asyncio.wait_for(done.wait(), timeout=30)

    report = acker.flush()
    assert report.settled_tags == total
    assert any(m for _, m in frames)  # at least one cumulative frame
    assert acker.coalesced_total >= 2
    assert acker.pending == 0
    await _await_until(lambda: _counts(rabbit, queue) == (0, 0))
    await broker.stop()
