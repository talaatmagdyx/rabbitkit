"""Basic async consumer — minimal rabbitkit setup.

Run:
    docker run -d --rm -p 5672:5672 rabbitmq:4-management
    python examples/basic_consumer/worker.py

In another terminal:
    python examples/basic_consumer/publish.py

Requirements:
    pip install "rabbitkit[async]"
"""

import asyncio

from rabbitkit import ConnectionConfig, RabbitConfig
from rabbitkit.aio import AsyncBroker

broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig(host="localhost")))


@broker.subscriber(queue="greetings", exchange="greetings", routing_key="greetings.say")
async def handle_greeting(body: bytes) -> None:
    print(f"[handler] {body.decode()}")


async def main() -> None:
    await broker.start()
    print("Waiting for messages (Ctrl+C to stop)...")
    try:
        while True:
            await asyncio.sleep(1)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
