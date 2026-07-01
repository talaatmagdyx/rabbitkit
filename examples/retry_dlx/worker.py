"""Consumer with RetryConfig — demonstrates retry + DLQ topology.

The handler always raises ValueError. rabbitkit will retry 3 times
(after 5 s, 30 s, 120 s) then dead-letter the message.

Run:
    docker compose up -d
    python examples/retry_dlx/worker.py

Inspect the DLQ after retries exhaust:
    rabbitkit dlq inspect --queue orders.queue.dlq

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import ConnectionConfig, RabbitConfig, RetryConfig
from rabbitkit.aio import AsyncBroker
from rabbitkit.core.types import AckPolicy
from rabbitkit.middleware.retry import RetryMiddleware

RETRY = RetryConfig(max_retries=3, delays=(5, 30, 120))

broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig(host="localhost"), retry=RETRY))


@broker.subscriber(
    queue="orders.queue",
    exchange="orders.exchange",
    routing_key="orders.process",
    ack_policy=AckPolicy.NACK_ON_ERROR,
    retry=RETRY,
    middlewares=[RetryMiddleware(RETRY, publish_async_fn=None)],
)
async def handle_order(body: bytes) -> None:
    print(f"[handler] processing: {body.decode()!r}")
    raise ValueError("simulated processing failure — will retry")


async def main() -> None:
    await broker.start()
    print("Waiting for messages (Ctrl+C to stop)...")
    print("After 3 retries, the message moves to orders.queue.dlq")
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
