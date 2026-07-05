"""Message handling: AckMessage / NackMessage / RejectMessage exceptions.

These sentinel exceptions give you inline settlement control in AUTO mode
without switching to MANUAL ack policy.

Run:
    python examples/message_handling/04_exception_settlement.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.message import AckMessage, NackMessage, RejectMessage

broker = AsyncBroker(RabbitConfig())


@broker.subscriber(queue="controlled-settlement")
async def handle_with_control(body: bytes) -> None:
    """
    Demonstrates inline settlement via exception-based control flow.
    All three sentinel exceptions are caught by the pipeline before
    reaching external error handlers.
    """
    try:
        data = json.loads(body)
    except json.JSONDecodeError:
        # Malformed JSON — reject immediately, don't requeue
        raise RejectMessage(requeue=False)

    action = data.get("action")

    if action == "ack":
        # Explicitly force ack (same as normal return, but declarative)
        print(f"[settlement] forcing ack for id={data.get('id')}")
        raise AckMessage()

    elif action == "nack_requeue":
        # Nack and requeue — will be re-delivered to another consumer
        print(f"[settlement] nacking with requeue for id={data.get('id')}")
        raise NackMessage(requeue=True)

    elif action == "nack_discard":
        # Nack without requeue — goes to DLQ if configured
        print(f"[settlement] nacking (discard) for id={data.get('id')}")
        raise NackMessage(requeue=False)

    elif action == "reject":
        # Reject — semantic equivalent of "unprocessable", goes to DLQ
        print(f"[settlement] rejecting for id={data.get('id')}")
        raise RejectMessage(requeue=False)

    else:
        # Normal processing
        print(f"[settlement] processed normally: action={action!r}")
        # Falls through → auto ack


# ─────────────────────────────────────────────────────────────────────────────
# Real-world pattern: conditional settlement based on business logic
# ─────────────────────────────────────────────────────────────────────────────

class TemporaryOutageError(Exception):
    """External service is temporarily unavailable."""

class InvalidPayloadError(Exception):
    """Message payload cannot be processed — permanent failure."""


@broker.subscriber(queue="business-logic")
async def handle_business_event(body: bytes) -> None:
    """Use exceptions to express business-level settlement decisions."""
    try:
        data = json.loads(body)
        if not data.get("user_id"):
            raise InvalidPayloadError("missing user_id")

        # Simulate calling an external API
        if data.get("simulate_outage"):
            raise TemporaryOutageError("payment service down")

        print(f"[business] processed user={data['user_id']}")

    except InvalidPayloadError as exc:
        print(f"[business] invalid payload: {exc} → rejecting")
        raise RejectMessage(requeue=False)

    except TemporaryOutageError as exc:
        print(f"[business] temporary outage: {exc} → nack+requeue")
        raise NackMessage(requeue=True)


async def main() -> None:
    await broker.start()

    test_cases = [
        {"id": 1, "action": "ack"},
        {"id": 2, "action": "nack_discard"},
        {"id": 3, "action": "reject"},
        {"id": 4, "action": "process"},
    ]
    for tc in test_cases:
        await broker.publish(MessageEnvelope(
            routing_key="controlled-settlement",
            body=json.dumps(tc).encode(),
        ))

    # Business logic examples
    await broker.publish(MessageEnvelope(
        routing_key="business-logic",
        body=json.dumps({"user_id": 42, "event": "purchase"}).encode(),
    ))
    await broker.publish(MessageEnvelope(
        routing_key="business-logic",
        body=json.dumps({"user_id": 7, "simulate_outage": True}).encode(),
    ))
    await broker.publish(MessageEnvelope(
        routing_key="business-logic",
        body=json.dumps({"event": "no_user_id"}).encode(),
    ))

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
