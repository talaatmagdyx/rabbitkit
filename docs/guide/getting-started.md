# Getting Started

## Install

```bash
pip install rabbitkit[async]   # async transport (aio-pika)
pip install rabbitkit[sync]    # sync transport (pika)
pip install rabbitkit[all]     # everything
```

## First Consumer

```python
import asyncio
from rabbitkit import AsyncBroker, RabbitConfig

broker = AsyncBroker(RabbitConfig())

@broker.subscriber(queue="orders")
async def handle(body: dict) -> None:
    print(f"Received: {body}")

async def main() -> None:
    await broker.start()
    await asyncio.Event().wait()  # run until SIGINT/SIGTERM

asyncio.run(main())
```

`AsyncBroker` can also be imported as `from rabbitkit.aio import AsyncBroker` — both paths are identical.

## Publish a Message

```python
# Simple kwargs form (recommended)
await broker.publish(routing_key="orders", body={"id": 1, "item": "widget"})

# With headers and explicit exchange
await broker.publish(
    routing_key="orders.created",
    body={"id": 1},
    exchange="events",
    headers={"x-tenant": "acme"},
)

# MessageEnvelope form (full control)
from rabbitkit import MessageEnvelope
await broker.publish(MessageEnvelope(routing_key="orders", body=b'{"id":1}'))
```

## Add Retry / DLQ

```python
from rabbitkit.core.config import RetryConfig

@broker.subscriber(queue="orders", retry_config=RetryConfig(max_retries=3, delays=(5, 30, 120)))
async def handle(body: dict) -> None:
    ...  # retried 3x on transient errors; dead-lettered after
```

## Test Without RabbitMQ

```python
from rabbitkit.testing import TestBroker

broker = TestBroker()

@broker.subscriber(queue="orders")
def handle(body: bytes) -> None:
    ...

broker.start()
broker.publish(MessageEnvelope(routing_key="orders", body=b"test"))
broker.assert_acked(broker.consumed_messages[0])
```

See the [Full Guide](full-guide.md) for middleware, DI, serialization, Kubernetes, and more.
