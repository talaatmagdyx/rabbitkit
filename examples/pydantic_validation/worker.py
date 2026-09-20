"""pydantic_validation — automatic Pydantic model validation on consume.

When a handler parameter is type-annotated with a Pydantic BaseModel,
rabbitkit automatically calls model_validate() on the JSON body.
Invalid messages are rejected with a permanent error (→ DLQ if configured).

Demonstrates:
- Pydantic v2 model as handler parameter
- Automatic JSON parse + validation
- Validation errors treated as permanent (no retry)
- Optional fields and defaults

Run:
    pip install rabbitkit[async] pydantic
    docker run -d -p 5672:5672 rabbitmq:3.13-management-alpine
    python worker.py
"""

# NOTE: deliberately NO `from __future__ import annotations` here.
# It makes every annotation a lazy string, so the pipeline's
# inspect.signature() read sees "Order" instead of the class and cannot
# decode the body into the model — the handler then receives raw bytes and
# fails with `'bytes' object has no attribute 'id'`.

import asyncio
import signal

from pydantic import BaseModel, Field, field_validator

from rabbitkit import AsyncBroker, RabbitConfig, RetryConfig
from rabbitkit.core.config import ConnectionConfig
from rabbitkit.serialization.json import JSONSerializer


class Order(BaseModel):
    id: int
    item: str
    qty: int = Field(default=1, ge=1)
    tenant: str = "default"

    @field_validator("item")
    @classmethod
    def item_not_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("item must not be empty")
        return v.strip()


config = RabbitConfig(
    connection=ConnectionConfig(host="localhost", port=5672),
    retry=RetryConfig(max_retries=3, delays=(5, 30, 120)),
)
# serializer= is what lets a `body: Order` handler receive a decoded model.
# Without it the pipeline hands the handler raw bytes.
broker = AsyncBroker(config, serializer=JSONSerializer())


@broker.subscriber(queue="pydantic.orders")
async def handle_order(body: Order) -> None:
    """Handler receives a fully-validated Order model."""
    print(f"order id={body.id} item={body.item!r} qty={body.qty} tenant={body.tenant!r}")


async def main() -> None:
    await broker.start()
    print("Listening on 'orders'. Send JSON with id/item/qty fields.")
    print("Press Ctrl+C to stop.")

    loop = asyncio.get_running_loop()
    stop = asyncio.Event()
    loop.add_signal_handler(signal.SIGINT, stop.set)
    loop.add_signal_handler(signal.SIGTERM, stop.set)

    await stop.wait()
    await broker.stop()
    print("stopped.")


if __name__ == "__main__":
    asyncio.run(main())
