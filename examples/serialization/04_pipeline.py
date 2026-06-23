"""Serialization: Two-stage SerializationPipeline.

Compose a Parser (bytes → dict) and a Decoder (dict → typed object)
independently to mix and match wire formats with type mappings.

Run:
    python examples/serialization/04_pipeline.py

Requirements:
    pip install "rabbitkit[async,pydantic]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import json
from dataclasses import dataclass

from pydantic import BaseModel

from rabbitkit import RabbitConfig, MessageEnvelope
from rabbitkit.async_ import AsyncBroker
from rabbitkit.serialization.pipeline import (
    SerializationPipeline,
    JsonParser,
    PydanticDecoder,
    DataclassDecoder,
    RawDecoder,
)


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline 1: JSON bytes → Pydantic model
# ─────────────────────────────────────────────────────────────────────────────

class User(BaseModel):
    id: int
    name: str
    role: str = "user"


pydantic_pipeline = SerializationPipeline(JsonParser(), PydanticDecoder())

broker_pydantic = AsyncBroker(RabbitConfig(), serializer=pydantic_pipeline)

@broker_pydantic.subscriber(queue="pipeline-pydantic")
async def handle_user(user: User) -> None:
    print(f"[pydantic-pipeline] user id={user.id} name={user.name!r} role={user.role!r}")


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline 2: JSON bytes → stdlib dataclass
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class Metric:
    name: str
    value: float
    unit: str = "count"


dataclass_pipeline = SerializationPipeline(JsonParser(), DataclassDecoder())

broker_dc = AsyncBroker(RabbitConfig(), serializer=dataclass_pipeline)

@broker_dc.subscriber(queue="pipeline-dataclass")
async def handle_metric(metric: Metric) -> None:
    print(f"[dc-pipeline] {metric.name}={metric.value} {metric.unit}")


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline 3: Raw pass-through (parse only, no type mapping)
# ─────────────────────────────────────────────────────────────────────────────

raw_pipeline = SerializationPipeline(JsonParser(), RawDecoder())

broker_raw = AsyncBroker(RabbitConfig(), serializer=raw_pipeline)

@broker_raw.subscriber(queue="pipeline-raw")
async def handle_raw(body: dict) -> None:  # type: ignore[type-arg]
    print(f"[raw-pipeline] dict: {body}")


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline 4: Custom msgpack parser (bring your own)
# ─────────────────────────────────────────────────────────────────────────────

class CustomJsonParserWithDefaults:
    """Custom parser that adds default keys before decoding."""

    def parse(self, data: bytes, content_type: str | None = None) -> dict:  # type: ignore[type-arg]
        parsed = json.loads(data)
        parsed.setdefault("source", "unknown")
        parsed.setdefault("version", 1)
        return parsed

    def serialize(self, data: object) -> bytes:
        return json.dumps(data, default=str).encode()

    @property
    def content_type(self) -> str:
        return "application/json"


custom_pipeline = SerializationPipeline(CustomJsonParserWithDefaults(), PydanticDecoder())


async def main() -> None:
    await broker_pydantic.start()
    await broker_dc.start()
    await broker_raw.start()

    # Pydantic pipeline
    await broker_pydantic.publish(MessageEnvelope(
        routing_key="pipeline-pydantic",
        body=json.dumps({"id": 7, "name": "Alice", "role": "admin"}).encode(),
        content_type="application/json",
    ))

    # Dataclass pipeline
    await broker_dc.publish(MessageEnvelope(
        routing_key="pipeline-dataclass",
        body=json.dumps({"name": "cpu_usage", "value": 72.4, "unit": "percent"}).encode(),
        content_type="application/json",
    ))

    # Raw pipeline
    await broker_raw.publish(MessageEnvelope(
        routing_key="pipeline-raw",
        body=json.dumps({"arbitrary": True, "keys": [1, 2, 3]}).encode(),
        content_type="application/json",
    ))

    await asyncio.sleep(0.5)
    await broker_pydantic.stop()
    await broker_dc.stop()
    await broker_raw.stop()


if __name__ == "__main__":
    asyncio.run(main())
