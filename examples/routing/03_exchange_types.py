"""Routing: Direct, Fanout, Topic, and Headers exchange types.

Demonstrates all four AMQP exchange types with real-world use cases.

Run:
    python examples/routing/03_exchange_types.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import ExchangeType

broker = AsyncBroker(RabbitConfig())

# ─────────────────────────────────────────────────────────────────────────────
# 1. DIRECT — messages go to queues whose binding key matches routing_key exactly
# ─────────────────────────────────────────────────────────────────────────────
direct_exchange = RabbitExchange(name="direct-demo", type=ExchangeType.DIRECT)

@broker.subscriber(queue="direct-high", exchange=direct_exchange, routing_key="high")
async def handle_high_priority(body: bytes) -> None:
    print(f"[direct/high] {body.decode()}")

@broker.subscriber(queue="direct-low", exchange=direct_exchange, routing_key="low")
async def handle_low_priority(body: bytes) -> None:
    print(f"[direct/low] {body.decode()}")


# ─────────────────────────────────────────────────────────────────────────────
# 2. FANOUT — broadcast to ALL bound queues (routing key ignored)
# ─────────────────────────────────────────────────────────────────────────────
fanout_exchange = RabbitExchange(name="fanout-demo", type=ExchangeType.FANOUT)

@broker.subscriber(queue="fanout-service-a", exchange=fanout_exchange)
async def handle_fanout_a(body: bytes) -> None:
    print(f"[fanout/service-a] {body.decode()}")

@broker.subscriber(queue="fanout-service-b", exchange=fanout_exchange)
async def handle_fanout_b(body: bytes) -> None:
    print(f"[fanout/service-b] {body.decode()}")


# ─────────────────────────────────────────────────────────────────────────────
# 3. TOPIC — pattern matching with * (one word) and # (zero or more words)
# ─────────────────────────────────────────────────────────────────────────────
topic_exchange = RabbitExchange(name="topic-demo", type=ExchangeType.TOPIC)

@broker.subscriber(queue="topic-us-all",   exchange=topic_exchange, routing_key="us.#")
async def handle_us_all(body: bytes) -> None:
    # matches us.east, us.west.sales, us.any.thing
    print(f"[topic/us.*] {body.decode()}")

@broker.subscriber(queue="topic-any-sales", exchange=topic_exchange, routing_key="*.sales")
async def handle_any_sales(body: bytes) -> None:
    # matches us.sales, eu.sales (exactly one word before .sales)
    print(f"[topic/*.sales] {body.decode()}")


# ─────────────────────────────────────────────────────────────────────────────
# 4. HEADERS — route based on message headers (routing key ignored)
# ─────────────────────────────────────────────────────────────────────────────
headers_exchange = RabbitExchange(name="headers-demo", type=ExchangeType.HEADERS)

@broker.subscriber(
    queue=RabbitQueue(
        name="headers-pdf",
        bind_arguments={"x-match": "all", "format": "pdf"},  # required for headers exchanges
    ),
    exchange=headers_exchange,
    routing_key="",  # ignored for headers exchange
)
async def handle_headers(body: bytes) -> None:
    # In practice you'd inspect msg.headers here
    print(f"[headers] {body.decode()}")


async def main() -> None:
    await broker.start()

    # Direct
    await broker.publish(MessageEnvelope(exchange="direct-demo", routing_key="high", body=b"urgent task"))
    await broker.publish(MessageEnvelope(exchange="direct-demo", routing_key="low",  body=b"background task"))

    # Fanout — both service-a and service-b receive this
    await broker.publish(MessageEnvelope(exchange="fanout-demo", routing_key="", body=b"broadcast event"))

    # Topic
    await broker.publish(MessageEnvelope(exchange="topic-demo", routing_key="us.east",       body=b"US east event"))
    await broker.publish(MessageEnvelope(exchange="topic-demo", routing_key="us.sales",       body=b"US sales event"))
    await broker.publish(MessageEnvelope(exchange="topic-demo", routing_key="eu.sales",       body=b"EU sales event"))
    await broker.publish(MessageEnvelope(exchange="topic-demo", routing_key="us.west.retail", body=b"US west retail"))

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
