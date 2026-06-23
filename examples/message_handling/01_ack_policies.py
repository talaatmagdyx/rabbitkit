"""Message handling: AckPolicy — AUTO, MANUAL, ACK_FIRST, NACK_ON_ERROR.

Demonstrates all four acknowledgement policies and when to use each.

Run:
    python examples/message_handling/01_ack_policies.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import RabbitConfig, MessageEnvelope
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import AckPolicy

broker = AsyncBroker(RabbitConfig())


# ─────────────────────────────────────────────────────────────────────────────
# AUTO (default) — ack on success, classify exception → nack/reject
# Use this for most handlers. Transient errors get nacked (requeued),
# permanent errors get rejected (DLQ).
# ─────────────────────────────────────────────────────────────────────────────
@broker.subscriber(queue="auto-ack-demo", ack_policy=AckPolicy.AUTO)
async def handle_auto(body: bytes) -> None:
    """AUTO: framework handles ack/nack based on exception type."""
    print(f"[AUTO] processing: {body.decode()}")
    # Success → auto ack
    # raise ValueError("bad input")  # permanent → reject (DLQ)
    # raise ConnectionError("timeout") # transient → nack (requeue)


# ─────────────────────────────────────────────────────────────────────────────
# MANUAL — handler owns ack/nack/reject entirely
# Use when you need fine-grained control, e.g. partial ack after saving
# to a database mid-handler.
# ─────────────────────────────────────────────────────────────────────────────
@broker.subscriber(queue="manual-ack-demo", ack_policy=AckPolicy.MANUAL)
async def handle_manual(msg: RabbitMessage) -> None:
    """MANUAL: handler decides when and how to settle the message."""
    print(f"[MANUAL] body: {msg.body.decode()}")
    try:
        # Simulate processing
        if msg.body == b"bad":
            await msg.nack_async(requeue=False)   # discard bad message
            return
        # ... do real work ...
        await msg.ack_async()
        print("[MANUAL] acked")
    except Exception:
        await msg.nack_async(requeue=True)        # requeue on unexpected error


# ─────────────────────────────────────────────────────────────────────────────
# ACK_FIRST — ack BEFORE the handler runs (at-most-once delivery)
# Use for side-effect-heavy handlers where duplicate processing is worse
# than lost messages (e.g., sending emails).
# ─────────────────────────────────────────────────────────────────────────────
@broker.subscriber(queue="ack-first-demo", ack_policy=AckPolicy.ACK_FIRST)
async def handle_ack_first(body: bytes) -> None:
    """ACK_FIRST: message is acked before handler runs — never re-delivered."""
    print(f"[ACK_FIRST] sending email to: {body.decode()}")
    # Even if this raises, the message is already acked — won't be re-delivered


# ─────────────────────────────────────────────────────────────────────────────
# NACK_ON_ERROR — ack on success, nack(requeue=False) on ANY exception
# Use when you always want failures to go to DLQ (no retry).
# ─────────────────────────────────────────────────────────────────────────────
@broker.subscriber(queue="nack-on-error-demo", ack_policy=AckPolicy.NACK_ON_ERROR)
async def handle_nack_on_error(body: bytes) -> None:
    """NACK_ON_ERROR: any exception → nack without requeue → DLQ."""
    print(f"[NACK_ON_ERROR] processing: {body.decode()}")
    # raise RuntimeError("oops") → nacked, not requeued


async def main() -> None:
    await broker.start()

    for queue in ["auto-ack-demo", "manual-ack-demo", "ack-first-demo", "nack-on-error-demo"]:
        await broker.publish(MessageEnvelope(routing_key=queue, body=f"message for {queue}".encode()))

    # Send a "bad" message to the manual handler
    await broker.publish(MessageEnvelope(routing_key="manual-ack-demo", body=b"bad"))

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
