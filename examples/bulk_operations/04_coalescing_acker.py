"""Bulk operations: safe ack coalescing with ``CoalescingAcker``.

Handlers finish OUT OF ORDER (random sleeps, 4 concurrent workers). The
coalescing acker knows every outstanding delivery on the channel and emits a
cumulative ``basic.ack(multiple=True)`` only through the longest prefix of
completed tags; anything stranded behind a still-running sibling is acked
individually after a short hold. Compare the log lines: ``multiple=True``
frames cover several tags, and no frame ever covers a tag whose handler is
still running.

Run:
    python examples/bulk_operations/04_coalescing_acker.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json
import random

from rabbitkit import (
    AckPolicy,
    AsyncBroker,
    BatchAckConfig,
    CoalescingAcker,
    MessageEnvelope,
    RabbitConfig,
    RabbitMessage,
    WorkerConfig,
)

QUEUE = "bulk-demo-coalescing"
TOTAL = 24

broker = AsyncBroker(RabbitConfig())
loop_ref: dict[str, asyncio.AbstractEventLoop] = {}
channel_ref: dict[str, object] = {}
frames: list[tuple[int, bool]] = []
done = asyncio.Event()
completed = 0


def _emit(coro_factory):
    """The acker's timer fires on a helper thread; aio-pika channel calls must
    run on the event loop, so hand the coroutine over thread-safely."""
    loop = loop_ref["loop"]
    loop.call_soon_threadsafe(lambda: loop.create_task(coro_factory()))


def ack_fn(tag: int, multiple: bool) -> None:
    frames.append((tag, multiple))
    print(f"  wire: basic.ack tag={tag} multiple={multiple}")
    _emit(lambda: channel_ref["channel"].basic_ack(delivery_tag=tag, multiple=multiple))  # type: ignore[attr-defined]


def nack_fn(tag: int, requeue: bool) -> None:
    _emit(lambda: channel_ref["channel"].basic_nack(delivery_tag=tag, requeue=requeue))  # type: ignore[attr-defined]


def reject_fn(tag: int, requeue: bool) -> None:
    _emit(lambda: channel_ref["channel"].basic_reject(delivery_tag=tag, requeue=requeue))  # type: ignore[attr-defined]


acker = CoalescingAcker(
    ack_fn=ack_fn,
    nack_fn=nack_fn,
    reject_fn=reject_fn,
    config=BatchAckConfig(batch_size=6, flush_interval_ms=150),
    max_hold=2,  # a completed tag stranded behind a slow sibling waits <= 2 rounds, then acks alone
)


@broker.subscriber(queue=QUEUE, ack_policy=AckPolicy.MANUAL)
async def handle(body: bytes, msg: RabbitMessage) -> None:
    global completed
    assert msg.delivery_tag is not None
    channel_ref.setdefault("channel", msg.raw_message.channel)  # aiormq channel the delivery arrived on
    acker.register(msg.delivery_tag)  # BEFORE doing work — the ledger must know every delivery

    await asyncio.sleep(random.uniform(0.0, 0.4))  # out-of-order completion
    n = json.loads(body)["n"]
    if n % 11 == 10:
        acker.fail(msg.delivery_tag, requeue=False)  # terminal → nack (dead-letters)
    else:
        acker.complete(msg.delivery_tag)
    completed += 1
    if completed >= TOTAL:
        done.set()


async def main() -> None:
    loop_ref["loop"] = asyncio.get_running_loop()
    await broker.start(worker_config=WorkerConfig(worker_count=4))
    await asyncio.sleep(0.3)

    await broker.publish_many(
        [MessageEnvelope(routing_key=QUEUE, body=json.dumps({"n": n}).encode()) for n in range(TOTAL)]
    )
    await asyncio.wait_for(done.wait(), timeout=30)
    await asyncio.sleep(0.5)  # let the interval timer flush the tail
    report = acker.close()
    await asyncio.sleep(0.3)  # let the marshalled frames hit the wire

    cumulative = [t for t, m in frames if m]
    print(
        f"\n{TOTAL} deliveries settled with {len(frames)} frames "
        f"({len(cumulative)} cumulative); coalesced tags={acker.coalesced_total}, "
        f"close flushed {report.settled_tags} tag(s)"
    )
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
