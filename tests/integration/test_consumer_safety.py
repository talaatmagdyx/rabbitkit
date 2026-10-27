"""Real-broker consumer-safety tests (settlement, poison bound, DLQ safety).

Each test reproduces a defect from the consumer reliability audit against a
live RabbitMQ and asserts what the BROKER reports, not what the client
believes it did.
"""

from __future__ import annotations

import asyncio
import uuid
from typing import Any

import pytest

from tests.integration.conftest import await_consumers, await_counts

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def rabbit(rabbit_container: dict[str, Any]) -> dict[str, Any]:
    return rabbit_container


def _config(url: str, **kw: Any) -> Any:
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    return RabbitConfig(connection=ConnectionConfig.from_url(url), **kw)


async def _publish_raw(url: str, queue: str, ids: list[str]) -> None:
    import aio_pika

    conn = await aio_pika.connect(url)
    async with conn:
        ch = await conn.channel(publisher_confirms=True)
        for mid in ids:
            await ch.default_exchange.publish(aio_pika.Message(body=b"{}", message_id=mid), routing_key=queue)


# ── S-D2: a cumulative ack never sweeps a refused delivery ────────────────


async def test_cumulative_ack_does_not_sweep_a_refused_delivery(rabbit: dict[str, Any]) -> None:
    """max_pending=2 refuses the 3rd delivery. After the first two settle and
    two more complete, the flush must not emit ack(5, multiple=True): that
    would acknowledge the refused delivery without it ever being processed.
    The broker must still hold it, and redeliver it after the consumer stops."""
    from rabbitkit import AckPolicy, AsyncBroker, AsyncCoalescingAcker, BatchAckConfig, RabbitMessage
    from rabbitkit.core.config import ConsumerConfig
    from rabbitkit.core.settlement import LedgerFullError

    queue = f"csafe-refused-{uuid.uuid4().hex[:8]}"
    broker = AsyncBroker(_config(rabbit["url"], consumer=ConsumerConfig(prefetch_count=10)))
    holder: dict[str, AsyncCoalescingAcker] = {}
    processed: list[str] = []
    refused: list[str] = []
    arrived = asyncio.Event()

    @broker.subscriber(queue=queue, ack_policy=AckPolicy.MANUAL)
    async def handle(body: bytes, msg: RabbitMessage) -> None:
        ch = msg.raw_message.channel
        acker = holder.setdefault(
            "acker",
            AsyncCoalescingAcker(
                ack_fn=lambda t, m: ch.basic_ack(t, multiple=m),
                nack_fn=lambda t, r: ch.basic_nack(t, requeue=r),
                reject_fn=lambda t, r: ch.basic_reject(t, requeue=r),
                config=BatchAckConfig(batch_size=1000, flush_interval_ms=0),
                max_pending=2,
            ),
        )
        assert msg.delivery_tag is not None
        try:
            acker.register(msg.delivery_tag, channel_key=ch)
        except LedgerFullError:
            refused.append(msg.message_id or "?")
        else:
            processed.append(msg.message_id or "?")
            acker.complete(msg.delivery_tag)
        if len(processed) + len(refused) in (3, 5):
            arrived.set()

    await broker.start(install_signal_handlers=False)
    try:
        await await_consumers(rabbit["url"], queue)
        await _publish_raw(rabbit["url"], queue, ["a", "b", "c"])
        await asyncio.wait_for(arrived.wait(), timeout=20)
        arrived.clear()
        await holder["acker"].flush()
        await _publish_raw(rabbit["url"], queue, ["d", "e"])
        await asyncio.wait_for(arrived.wait(), timeout=20)
        report = await holder["acker"].flush()
        assert refused == ["c"]
        assert not any(c.multiple for c in report.commands), report.commands
        await await_counts(rabbit, queue, (0, 1), timeout=20)
    finally:
        await broker.stop()
    await await_counts(rabbit, queue, (1, 0), timeout=20)


def _ready(url: str, queue: str) -> int | None:
    """Ready count by passive declare (works for quorum queues, whose
    ``rabbitmqctl list_queues`` counts can lag)."""
    from tests.integration.conftest import QueueProbe

    with QueueProbe(url) as probe:
        return probe.ready(queue)


async def _await_ready(url: str, queue: str, expected: int, timeout: float = 20.0) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    seen: int | None = None
    while loop.time() < deadline:
        seen = await asyncio.to_thread(_ready, url, queue)
        if seen == expected:
            return
        await asyncio.sleep(0.1)
    raise AssertionError(f"{queue}: expected {expected} ready, last saw {seen}")


# ── F15: the default transient-failure loop is bounded ───────────────────


@pytest.mark.parametrize("queue_type", ["classic", "quorum"])
async def test_transient_poison_is_dead_lettered_after_one_requeue(rabbit: dict[str, Any], queue_type: str) -> None:
    """No retry configured, handler always raises TimeoutError: the default
    requeues once, then dead-letters the redelivery. Previously it hot-looped
    (thousands of runs per second, never reaching the DLQ)."""
    from rabbitkit import AsyncBroker, RabbitMessage, RabbitQueue
    from rabbitkit.core.types import QueueType

    queue = f"csafe-poison-{queue_type}-{uuid.uuid4().hex[:8]}"
    qtype = QueueType.QUORUM if queue_type == "quorum" else QueueType.CLASSIC
    broker = AsyncBroker(_config(rabbit["url"]))
    calls: list[bool] = []

    @broker.subscriber(RabbitQueue(name=queue, queue_type=qtype, durable=True))
    async def handle(body: bytes, msg: RabbitMessage) -> None:
        calls.append(msg.redelivered)
        raise TimeoutError("downstream timed out")

    await broker.start(install_signal_handlers=False)
    try:
        await await_consumers(rabbit["url"], queue)
        await _publish_raw(rabbit["url"], queue, ["poison"])
        await _await_ready(rabbit["url"], f"{queue}.dlq", 1)
        await asyncio.sleep(0.5)  # would be thousands of extra runs under the old default
        assert calls == [False, True]
        assert await asyncio.to_thread(_ready, rabbit["url"], queue) == 0
    finally:
        await broker.stop()


async def test_transient_opt_out_keeps_requeueing(rabbit: dict[str, Any]) -> None:
    from rabbitkit import AsyncBroker
    from rabbitkit.core.config import ConsumerConfig

    queue = f"csafe-optout-{uuid.uuid4().hex[:8]}"
    broker = AsyncBroker(_config(rabbit["url"], consumer=ConsumerConfig(reject_transient_on_redelivery=False)))
    calls = 0
    enough = asyncio.Event()

    @broker.subscriber(queue=queue)
    async def handle(body: bytes) -> None:
        nonlocal calls
        calls += 1
        if calls >= 5:
            enough.set()
        raise TimeoutError("downstream timed out")

    await broker.start(install_signal_handlers=False)
    try:
        await await_consumers(rabbit["url"], queue)
        await _publish_raw(rabbit["url"], queue, ["poison"])
        await asyncio.wait_for(enough.wait(), timeout=20)
        assert await asyncio.to_thread(_ready, rabbit["url"], f"{queue}.dlq") == 0
    finally:
        await broker.stop()


# ── MANUAL: a raising handler no longer pins prefetch slots ─────────────


async def test_manual_handler_failures_do_not_stall_the_consumer(rabbit: dict[str, Any]) -> None:
    """prefetch=2, four deliveries, MANUAL handler raises without settling.
    Previously two failures held both prefetch slots and the consumer
    stalled; now each failure is dead-lettered and all four are seen."""
    from rabbitkit import AckPolicy, AsyncBroker, RabbitMessage
    from rabbitkit.core.config import ConsumerConfig

    queue = f"csafe-manual-{uuid.uuid4().hex[:8]}"
    broker = AsyncBroker(_config(rabbit["url"], consumer=ConsumerConfig(prefetch_count=2)))
    seen: list[str] = []

    @broker.subscriber(queue=queue, ack_policy=AckPolicy.MANUAL)
    async def handle(body: bytes, msg: RabbitMessage) -> None:
        seen.append(msg.message_id or "?")
        raise RuntimeError("bug before settling")

    await broker.start(install_signal_handlers=False)
    try:
        await await_consumers(rabbit["url"], queue)
        await _publish_raw(rabbit["url"], queue, ["a", "b", "c", "d"])
        await _await_ready(rabbit["url"], f"{queue}.dlq", 4)
        assert sorted(seen) == ["a", "b", "c", "d"]
        await await_counts(rabbit, queue, (0, 0), timeout=20)
    finally:
        await broker.stop()


# ── F17: a missing DLQ never swallows a rejected message ────────────────


async def test_missing_dlq_does_not_lose_a_rejected_message(rabbit: dict[str, Any]) -> None:
    """Classic source queue with the default auto-provisioned DLQ, which is
    then deleted. A permanent failure must NOT reject into the void: the
    message stays on the source queue."""
    import aio_pika

    from rabbitkit import AsyncBroker

    queue = f"csafe-nodlq-{uuid.uuid4().hex[:8]}"
    broker = AsyncBroker(_config(rabbit["url"]))
    calls = 0
    attempted = asyncio.Event()

    @broker.subscriber(queue=queue)
    async def handle(body: bytes) -> None:
        nonlocal calls
        calls += 1
        attempted.set()
        raise ValueError("permanent")

    await broker.start(install_signal_handlers=False)
    try:
        conn = await aio_pika.connect(rabbit["url"])
        async with conn:
            ch = await conn.channel()
            await ch.queue_delete(f"{queue}.dlq")
        await await_consumers(rabbit["url"], queue)
        await _publish_raw(rabbit["url"], queue, ["precious"])
        await asyncio.wait_for(attempted.wait(), timeout=20)
        await asyncio.sleep(1.0)
    finally:
        await broker.stop()
    assert calls >= 1
    await await_counts(rabbit, queue, (1, 0), timeout=20)


# ── CONSUMER-006: processing deadline produces an explicit outcome ──────


async def test_processing_timeout_cancels_and_dead_letters_on_the_second_attempt(
    rabbit: dict[str, Any], caplog: pytest.LogCaptureFixture, capsys: pytest.CaptureFixture[str]
) -> None:
    from rabbitkit import AsyncBroker
    from rabbitkit.core.config import ConsumerConfig

    queue = f"csafe-deadline-{uuid.uuid4().hex[:8]}"
    broker = AsyncBroker(_config(rabbit["url"], consumer=ConsumerConfig(processing_timeout=0.3)))
    cancelled = 0

    @broker.subscriber(queue=queue)
    async def handle(body: bytes) -> None:
        nonlocal cancelled
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            cancelled += 1
            raise

    await broker.start(install_signal_handlers=False)
    try:
        await await_consumers(rabbit["url"], queue)
        with caplog.at_level("WARNING"):
            await _publish_raw(rabbit["url"], queue, ["slow"])
            await _await_ready(rabbit["url"], f"{queue}.dlq", 1)
        assert cancelled == 2
        text = caplog.text + capsys.readouterr().out  # stdlib or structlog rendering
        assert "error=HandlerTimeoutError" in text and "outcome_unknown=True" in text
    finally:
        await broker.stop()


# ── INV-008: stop() is bounded even when a handler ignores cancellation ─


async def test_stop_is_bounded_when_a_handler_swallows_cancellation(rabbit: dict[str, Any]) -> None:
    import time

    from rabbitkit import AsyncBroker
    from rabbitkit.core.config import ConsumerConfig

    queue = f"csafe-stop-{uuid.uuid4().hex[:8]}"
    broker = AsyncBroker(_config(rabbit["url"], consumer=ConsumerConfig(graceful_timeout=0.5)))
    started = asyncio.Event()

    @broker.subscriber(queue=queue)
    async def handle(body: bytes) -> None:
        started.set()
        try:
            await asyncio.sleep(30)
        except asyncio.CancelledError:
            await asyncio.sleep(5)  # misbehaving: swallows cancellation

    await broker.start(install_signal_handlers=False)
    await await_consumers(rabbit["url"], queue)
    await _publish_raw(rabbit["url"], queue, ["stuck"])
    await asyncio.wait_for(started.wait(), timeout=20)
    t0 = time.monotonic()
    await broker.stop()
    assert time.monotonic() - t0 < 3.0
    await await_counts(rabbit, queue, (1, 0), timeout=20)
