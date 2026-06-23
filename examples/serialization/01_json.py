"""Serialization: Built-in JSON serializer.

JsonSerializer handles encode/decode for dict, list, str, int, float,
and any JSON-serializable type. Uses stdlib json.

Run:
    python examples/serialization/01_json.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json

from rabbitkit import RabbitConfig, MessageEnvelope
from rabbitkit.async_ import AsyncBroker
from rabbitkit.serialization.json import JsonSerializer

# Attach serializer at broker level — all routes share it
broker = AsyncBroker(RabbitConfig(), serializer=JsonSerializer())


@broker.subscriber(queue="json-events")
async def handle_dict(body: dict) -> None:  # type: ignore[type-arg]
    """Body is automatically decoded from JSON to a Python dict."""
    print(f"[json] event={body.get('event')!r}, user={body.get('user_id')!r}")


@broker.subscriber(queue="json-lists")
async def handle_list(body: list) -> None:  # type: ignore[type-arg]
    """Body decoded to a Python list."""
    print(f"[json-list] {len(body)} items: {body[:3]}...")


# Per-route serializer override
custom_serializer = JsonSerializer()

@broker.subscriber(queue="json-raw", serializer=None)  # override: no serializer
async def handle_raw_bytes(body: bytes) -> None:
    """No serializer on this route — raw bytes arrive."""
    data = json.loads(body)  # manual parse
    print(f"[raw] manually parsed: {data}")


async def main() -> None:
    await broker.start()

    # The JsonSerializer encodes on publish too (via MessageEnvelope.body)
    # For broker.publish we pass bytes directly — the handler decodes it

    await broker.publish(MessageEnvelope(
        routing_key="json-events",
        body=json.dumps({"event": "user.signup", "user_id": 42}).encode(),
        content_type="application/json",
    ))

    await broker.publish(MessageEnvelope(
        routing_key="json-lists",
        body=json.dumps(["apple", "banana", "cherry", "date"]).encode(),
        content_type="application/json",
    ))

    await broker.publish(MessageEnvelope(
        routing_key="json-raw",
        body=json.dumps({"raw": True}).encode(),
    ))

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
