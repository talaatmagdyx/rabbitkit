"""Message handling: RabbitMessage — accessing headers, properties, and body.

Shows how to work with the full RabbitMessage object instead of just bytes.
Demonstrates headers, AMQP properties, routing info, and delivery metadata.

Run:
    python examples/message_handling/02_rabbit_message.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import AckPolicy

broker = AsyncBroker(RabbitConfig())


@broker.subscriber(queue="rich-messages", ack_policy=AckPolicy.MANUAL)
async def handle_rich_message(msg: RabbitMessage) -> None:
    """Inspect every field of the incoming RabbitMessage."""

    print("=" * 50)
    print("--- Body ---")
    print(f"  raw bytes : {msg.body!r}")
    try:
        print(f"  as JSON   : {json.loads(msg.body)}")
    except Exception:
        pass

    print("\n--- Routing Info ---")
    print(f"  routing_key   : {msg.routing_key!r}")
    print(f"  exchange      : {msg.exchange!r}")

    print("\n--- AMQP Properties ---")
    print(f"  message_id    : {msg.message_id!r}")
    print(f"  correlation_id: {msg.correlation_id!r}")
    print(f"  reply_to      : {msg.reply_to!r}")
    print(f"  content_type  : {msg.content_type!r}")
    print(f"  content_enc   : {msg.content_encoding!r}")
    print(f"  priority      : {msg.priority!r}")
    print(f"  expiration    : {msg.expiration!r}")
    print(f"  timestamp     : {msg.timestamp!r}")
    print(f"  type          : {msg.type!r}")
    print(f"  user_id       : {msg.user_id!r}")
    print(f"  app_id        : {msg.app_id!r}")

    print("\n--- Delivery Metadata ---")
    print(f"  delivery_tag  : {msg.delivery_tag!r}")
    print(f"  consumer_tag  : {msg.consumer_tag!r}")
    print(f"  redelivered   : {msg.redelivered!r}")
    print(f"  is_settled    : {msg.is_settled!r}")

    print("\n--- Custom Headers ---")
    for key, value in msg.headers.items():
        print(f"  {key}: {value!r}")

    print("\n--- Path (topic wildcard segments) ---")
    print(f"  path: {msg.path!r}")
    print("=" * 50)

    await msg.ack_async()


async def main() -> None:
    await broker.start()

    import uuid
    from datetime import datetime, timezone

    await broker.publish(
        MessageEnvelope(
            routing_key="rich-messages",
            body=json.dumps({"user": "alice", "action": "login"}).encode(),
            message_id=str(uuid.uuid4()),
            correlation_id="req-001",
            content_type="application/json",
            headers={
                "x-tenant": "acme",
                "x-source-service": "auth-service",
                "x-retry-count": 0,
            },
            type="UserEvent",
            app_id="auth-service",
        )
    )

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
