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
