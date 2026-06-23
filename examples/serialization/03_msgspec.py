"""Serialization: msgspec — high-performance binary/JSON serialization.

msgspec Struct is 10-100x faster than Pydantic for decode/encode.
Supports JSON and MessagePack wire formats.

Run:
    python examples/serialization/03_msgspec.py

Requirements:
    pip install "rabbitkit[async,msgspec]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json

from rabbitkit import RabbitConfig, MessageEnvelope
from rabbitkit.async_ import AsyncBroker

try:
    import msgspec

    from rabbitkit.serialization.msgspec import MsgspecSerializer

    # ── JSON mode (default) ───────────────────────────────────────────────────
    json_broker = AsyncBroker(
        RabbitConfig(),
        serializer=MsgspecSerializer(format="json"),
    )

    # ── Define msgspec Struct models ──────────────────────────────────────────

    class Order(msgspec.Struct):
        id: int
        item: str
        qty: int
        price: float

    class Event(msgspec.Struct):
        type: str
        payload: dict  # type: ignore[type-arg]
        version: int = 1


    @json_broker.subscriber(queue="msgspec-orders")
    async def handle_order(order: Order) -> None:
        """Decoded from JSON bytes into a validated Order struct."""
        total = order.qty * order.price
        print(f"[msgspec] Order #{order.id}: {order.qty}× {order.item!r} = ${total:.2f}")


    @json_broker.subscriber(queue="msgspec-events")
    async def handle_event(event: Event) -> None:
        print(f"[msgspec] Event type={event.type!r} v{event.version}: {event.payload}")


    async def main() -> None:
        await json_broker.start()

        await json_broker.publish(MessageEnvelope(
            routing_key="msgspec-orders",
            body=json.dumps({"id": 1, "item": "Sprocket", "qty": 4, "price": 3.75}).encode(),
            content_type="application/json",
        ))

        await json_broker.publish(MessageEnvelope(
            routing_key="msgspec-events",
            body=json.dumps({"type": "checkout", "payload": {"cart_total": 15.0}, "version": 2}).encode(),
            content_type="application/json",
        ))

        # Invalid struct — missing required field 'item' → DecodeError → DLQ
        await json_broker.publish(MessageEnvelope(
            routing_key="msgspec-orders",
            body=json.dumps({"id": 99, "qty": 1, "price": 5.0}).encode(),  # missing 'item'
            content_type="application/json",
        ))

        await asyncio.sleep(0.5)
        await json_broker.stop()

except ImportError:
    print("msgspec not installed — run: pip install 'rabbitkit[msgspec]'")

    async def main() -> None:  # type: ignore[misc]
        pass


if __name__ == "__main__":
    asyncio.run(main())
