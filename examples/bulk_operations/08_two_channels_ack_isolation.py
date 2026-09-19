"""Bulk operations: per-channel ack isolation with ``CoalescingAckerGroup``.

rabbitkit gives every subscriber queue its own channel, and delivery tags are
a PER-CHANNEL counter — tag 7 on channel A and tag 7 on channel B are
different messages. So a multi-queue consumer needs one ledger per channel::

    Channel A → CoalescingAcker A → SettlementCoordinator A
    Channel B → CoalescingAcker B → SettlementCoordinator B

``CoalescingAckerGroup`` keeps that isolation for you: it builds one acker
per channel from your factory and every call carries the channel, so a
cross-channel mistake raises ``ChannelMismatchError`` at registration instead
of acking the wrong messages. This script publishes the same number of
messages to two queues (both therefore carry tags 1..N), completes them out
of order, and prints the frames each channel emitted — you will see
cumulative acks on both, and never a tag covered by the wrong channel.

Run:
    python examples/bulk_operations/08_two_channels_ack_isolation.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import random
from typing import Any

from rabbitkit import (
    AckPolicy,
    AsyncBroker,
    BatchAckConfig,
    ChannelMismatchError,
    CoalescingAcker,
    CoalescingAckerGroup,
    ConsumerConfig,
    MessageEnvelope,
    RabbitConfig,
    RabbitMessage,
    WorkerConfig,
)

QUEUE_A = "bulk-demo-iso-orders"
QUEUE_B = "bulk-demo-iso-payments"
PER_QUEUE = 20

broker = AsyncBroker(RabbitConfig(consumer=ConsumerConfig(prefetch_count=64)))
loop_ref: dict[str, asyncio.AbstractEventLoop] = {}
frames: dict[str, list[tuple[int, bool]]] = {}
labels: dict[Any, str] = {}
settled = 0
done = asyncio.Event()


def make_acker(channel: Any) -> CoalescingAcker:
    """Build the acker that owns ONE channel. Its emit callables are bound to
    that channel, and ``channel_key`` makes the binding enforced."""
    label = labels.setdefault(channel, f"channel-{len(labels) + 1}")
    frames.setdefault(label, [])
    loop = loop_ref["loop"]

    def emit(coro: Any) -> None:
        loop.create_task(coro)

    def ack_fn(tag: int, multiple: bool) -> None:
        frames[label].append((tag, multiple))
        print(f"  {label}: basic.ack tag={tag} multiple={multiple}")
        emit(channel.basic_ack(delivery_tag=tag, multiple=multiple))

    return CoalescingAcker(
        ack_fn=ack_fn,
        nack_fn=lambda t, r: emit(channel.basic_nack(delivery_tag=t, requeue=r)),
        reject_fn=lambda t, r: emit(channel.basic_reject(delivery_tag=t, requeue=r)),
        config=BatchAckConfig(batch_size=1000, flush_interval_ms=200),
        channel_key=channel,  # ← turns "one acker per channel" into an invariant
    )


group = CoalescingAckerGroup(factory=make_acker)


async def process(msg: RabbitMessage) -> None:
    """Same body for both queues — the group routes to the right ledger."""
    global settled
    channel = msg.raw_message.channel  # the aiormq channel this delivery arrived on
    group.register(channel, msg.delivery_tag)  # register BEFORE doing work
    await asyncio.sleep(random.uniform(0, 0.3))  # out-of-order completion
    group.complete(channel, msg.delivery_tag)
    settled += 1
    if settled >= 2 * PER_QUEUE:
        done.set()


@broker.subscriber(queue=QUEUE_A, ack_policy=AckPolicy.MANUAL)
async def handle_orders(body: bytes, msg: RabbitMessage) -> None:
    await process(msg)


@broker.subscriber(queue=QUEUE_B, ack_policy=AckPolicy.MANUAL)
async def handle_payments(body: bytes, msg: RabbitMessage) -> None:
    await process(msg)


def show_guard() -> None:
    """What the guard prevents, without a broker in the way."""

    class FakeChannel:
        def __repr__(self) -> str:
            return "<other channel>"

    acker = group.for_channel(next(iter(labels)))
    try:
        acker.register(999, channel_key=FakeChannel())
    except ChannelMismatchError as exc:
        print(f"\nguard: {exc}")


async def main() -> None:
    loop_ref["loop"] = asyncio.get_running_loop()
    await broker.start(worker_config=WorkerConfig(worker_count=4))
    await asyncio.sleep(0.3)

    envelopes = [MessageEnvelope(routing_key=q, body=b"{}") for q in (QUEUE_A, QUEUE_B) for _ in range(PER_QUEUE)]
    (await broker.publish_many(envelopes)).raise_for_status()
    print(f"published {PER_QUEUE} messages to each of 2 queues; both carry tags 1..{PER_QUEUE}\n")

    await asyncio.wait_for(done.wait(), timeout=60)
    report = group.flush()
    await asyncio.sleep(0.3)  # let the marshalled frames reach the wire

    print(f"\nchannels with their own ledger: {group.channels}")
    for label, emitted in frames.items():
        cumulative = sum(1 for _, multiple in emitted if multiple)
        covered = max(tag for tag, _ in emitted)
        print(
            f"  {label}: {len(emitted)} frames ({cumulative} cumulative), "
            f"highest tag covered = {covered} (never above {PER_QUEUE})"
        )
    print(
        f"total settled={group.settled_total} coalesced={group.coalesced_total} "
        f"pending={group.pending} (last flush settled {report.settled_tags})"
    )

    show_guard()
    group.close()
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
