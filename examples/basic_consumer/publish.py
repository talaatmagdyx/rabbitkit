"""Publish a test message to the greetings queue.

Run after starting worker.py:
    python examples/basic_consumer/publish.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json

from rabbitkit import ConnectionConfig, MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker

broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig(host="localhost")))


async def main() -> None:
    await broker.start()
    outcome = await broker.publish(
        MessageEnvelope(
            exchange="greetings",
            routing_key="greetings.say",
            body=json.dumps({"message": "Hello"}).encode(),
            content_type="application/json",
        )
    )
    print(f"[publisher] {outcome.status.value}")
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
