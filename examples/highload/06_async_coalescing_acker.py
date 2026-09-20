"""High-load: AsyncCoalescingAcker — safe cumulative acks on the event loop.

One `basic_ack(tag, multiple=True)` can settle a whole run of deliveries, so
a busy consumer sends far fewer frames. The danger is that a cumulative ack
settles EVERYTHING still unacknowledged up to that tag — including a delivery
you meant to requeue.

Two rules make it safe, and the acker enforces both:

1. Only coalesce through a completed prefix. A still-running sibling is never
   acked early.
2. Plan, emit, then commit. Frames go out in order and emission stops at the
   first failure, so a cumulative ack can never follow a nack that did not
   land. The ledger advances only for frames the broker accepted.

Use the ASYNC acker on aio-pika. Settlement there is a coroutine, so a
synchronous callable could only schedule it and never learn whether it
landed — which is exactly the evidence rule 2 needs. It also means no timer
thread and no `marshal=`.

Run:
    python examples/highload/06_async_coalescing_acker.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import MessageEnvelope, RabbitConfig, RabbitMessage
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.config import BatchAckConfig, ConsumerConfig
from rabbitkit.core.types import AckPolicy
from rabbitkit.highload import AsyncCoalescingAcker

QUEUE = "coalesced-orders"
TOTAL = 200

broker = AsyncBroker(RabbitConfig(consumer=ConsumerConfig(prefetch_count=50)))

# One acker per channel: delivery tags are a per-channel counter, so feeding
# two channels into one acker would settle the wrong messages.
acker: AsyncCoalescingAcker | None = None
done = asyncio.Event()
seen = 0


def _build_acker(raw: object) -> AsyncCoalescingAcker:
    """Bind the three emit callables to one raw AMQP channel.

    `msg.raw_message.channel` is the aiormq channel the delivery arrived on.
    Its basic_ack/basic_nack/basic_reject are coroutines — which is the whole
    reason the async acker exists.
    """

    async def ack(tag: int, multiple: bool) -> None:
        await raw.basic_ack(delivery_tag=tag, multiple=multiple)

    async def nack(tag: int, requeue: bool) -> None:
        await raw.basic_nack(delivery_tag=tag, requeue=requeue)

    async def reject(tag: int, requeue: bool) -> None:
        await raw.basic_reject(delivery_tag=tag, requeue=requeue)

    return AsyncCoalescingAcker(
        ack_fn=ack,
        nack_fn=nack,
        reject_fn=reject,
        # Flush on whichever comes first: a full batch, or the interval.
        config=BatchAckConfig(batch_size=50, flush_interval_ms=200),
        channel_key=raw,
    )


@broker.subscriber(queue=QUEUE, ack_policy=AckPolicy.MANUAL)
async def handle_order(body: bytes, msg: RabbitMessage) -> None:
    """Report the outcome to the acker; never settle the message directly.

    AckPolicy.MANUAL logs a warning when a handler returns without settling.
    That is expected here: settlement is deliberately deferred to the acker's
    next flush, which is the whole point of coalescing.
    """
    global acker, seen
    if acker is None:
        acker = _build_acker(msg.raw_message.channel)
        await acker.start()

    acker.register(msg.delivery_tag, channel_key=msg.raw_message.channel)
    try:
        payload = body.decode()
        if payload.endswith("7"):
            # A poison record: requeue it. The acker emits an individual nack
            # BEFORE any cumulative ack above it, and if that nack fails it
            # withholds the rest rather than letting them swallow this tag.
            acker.fail(msg.delivery_tag, requeue=False)
        else:
            acker.complete(msg.delivery_tag)
    except Exception:
        acker.fail(msg.delivery_tag, requeue=True)
        raise

    await acker.maybe_flush()
    seen += 1
    if seen >= TOTAL:
        done.set()


async def main() -> None:
    global acker
    await broker.start()

    # Start from a known state: a reused local broker accumulates durable
    # queues, and leftover messages would make the printed counts confusing.
    await broker._transport.purge_queue(QUEUE)

    for i in range(TOTAL):
        await broker.publish(MessageEnvelope(routing_key=QUEUE, body=f"order-{i}".encode()))
    print(f"published {TOTAL} orders")

    try:
        await asyncio.wait_for(done.wait(), timeout=20)
    except TimeoutError:
        print(f"timed out after {seen}/{TOTAL}")

    if acker is not None:
        # close() drains what is provably safe and deliberately leaves any
        # unfinished delivery unacknowledged, for the broker to redeliver.
        report = await acker.close()
        frames = len(report.ok_commands)
        print(f"settled {acker.settled_total} deliveries")
        print(f"  cumulative coalescing saved {acker.coalesced_total} frames")
        print(f"  final flush sent {frames} frame(s)")
        print(f"  unresolved (left for redelivery): {acker.unresolved_total}")

    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
