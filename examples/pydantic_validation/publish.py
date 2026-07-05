"""Publish test messages — valid and invalid — to trigger pydantic_validation worker."""

from __future__ import annotations

import asyncio

from rabbitkit import AsyncBroker, RabbitConfig
from rabbitkit.core.config import ConnectionConfig


async def main() -> None:
    broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig(host="localhost")))
    await broker.start()

    messages = [
        {"id": 1, "item": "widget", "qty": 2, "tenant": "acme"},   # valid
        {"id": 2, "item": "gadget"},                                  # valid (qty defaults to 1)
        {"id": 3, "item": "", "qty": 1},                             # invalid: empty item
        {"id": 4, "qty": 5},                                          # invalid: missing item
    ]

    for msg in messages:
        await broker.publish(routing_key="pydantic.orders", body=msg)
        print(f"sent: {msg}")

    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
