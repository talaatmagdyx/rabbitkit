"""Two-stage serialization pipeline (parser + decoder).

Splits serialization into two composable stages so you can mix and match
wire formats with type-mapping strategies independently.

  Stage 1 — **Parser** (``MessageParser`` protocol)
    raw ``bytes``  →  intermediate form (usually ``dict`` or ``list``)
    Examples: ``JsonParser``, ``MsgpackParser`` (bring your own)

  Stage 2 — **Decoder** (``MessageDecoder`` protocol)
    intermediate  →  typed Python object
    Examples: ``PydanticDecoder``, ``DataclassDecoder``, ``RawDecoder``

``SerializationPipeline`` composes them and exposes the same ``encode`` /
``decode`` / ``content_type`` interface as built-in serializers, so it plugs
directly into ``broker(serializer=...)`` or ``@broker.subscriber(serializer=...)``.

Quick start — JSON + Pydantic
------------------------------
    from pydantic import BaseModel
    from rabbitkit.serialization.pipeline import (
        SerializationPipeline, JsonParser, PydanticDecoder,
    )

    class Order(BaseModel):
        id: int
        item: str
        qty: int

    pipeline = SerializationPipeline(JsonParser(), PydanticDecoder())

    broker = AsyncBroker(config, serializer=pipeline)

    @broker.subscriber(queue="orders")
    async def handle_order(order: Order) -> None:
        # `order` is already a validated Pydantic model
        print(order.id, order.item)

JSON + stdlib dataclass
-----------------------
    from dataclasses import dataclass
    from rabbitkit.serialization.pipeline import (
        SerializationPipeline, JsonParser, DataclassDecoder,
    )

    @dataclass
    class Event:
        type: str
        payload: dict

    pipeline = SerializationPipeline(JsonParser(), DataclassDecoder())

    @broker.subscriber(queue="events", serializer=pipeline)
    def handle(event: Event) -> None:
        print(event.type)

Pass-through (raw bytes, no decoding)
--------------------------------------
    from rabbitkit.serialization.pipeline import (
        SerializationPipeline, JsonParser, RawDecoder,
    )

    # Still parses bytes → dict for intermediate, but decoder returns as-is
    pipeline = SerializationPipeline(JsonParser(), RawDecoder())

Custom parser (msgpack example)
---------------------------------
    import msgpack

    class MsgpackParser:
        def parse(self, data: bytes, content_type=None):
            return msgpack.unpackb(data, raw=False)

        def serialize(self, data) -> bytes:
            return msgpack.packb(data, use_bin_type=True)

        @property
        def content_type(self) -> str:
            return "application/msgpack"

    pipeline = SerializationPipeline(MsgpackParser(), PydanticDecoder())

Encoding (publish direction)
-----------------------------
``pipeline.encode(my_model)`` calls ``decoder.encode(model) → dict`` then
``parser.serialize(dict) → bytes``.  This means the same pipeline handles
both inbound *and* outbound serialization transparently.
"""

from __future__ import annotations

import json
from typing import Any, Protocol


class MessageParser(Protocol):
    """Stage 1: raw bytes → intermediate form."""

    def parse(self, data: bytes, content_type: str | None = None) -> Any: ...
    def serialize(self, data: Any) -> bytes: ...
    @property
    def content_type(self) -> str: ...


class MessageDecoder(Protocol):
    """Stage 2: intermediate → typed Python object."""

    def decode(self, data: Any, target_type: type) -> Any: ...
    def encode(self, data: Any) -> Any: ...


class SerializationPipeline:
    """Two-stage serialization. Implements Serializer protocol."""

    def __init__(self, parser: MessageParser, decoder: MessageDecoder) -> None:
        self._parser = parser
        self._decoder = decoder

    def encode(self, data: Any) -> bytes:
        intermediate = self._decoder.encode(data)
        return self._parser.serialize(intermediate)

    def decode(self, data: bytes, target_type: type) -> Any:
        intermediate = self._parser.parse(data)
        return self._decoder.decode(intermediate, target_type)

    @property
    def content_type(self) -> str:
        return self._parser.content_type


class JsonParser:
    """Built-in JSON parser.

    By default ``serialize`` **raises** on objects ``json`` cannot represent
    (e.g. ``datetime``, ``Decimal``) rather than silently coercing them via
    ``str()``. Pass ``coerce_unknown_to_str=True`` to restore the legacy
    ``default=str`` coercion behaviour.

    M7: caps the input size before ``json.loads`` (64 MiB by default,
    matching ``CompressionMiddleware``'s ``max_decompressed_size`` default)
    -- without it, a large uncompressed body is fully materialized with no
    bound. Pass ``max_parse_bytes=None`` to opt out.
    """

    #: M7: see JSONSerializer's identical default for the rationale.
    _DEFAULT_MAX_PARSE_BYTES = 64 * 1024 * 1024

    def __init__(
        self,
        *,
        coerce_unknown_to_str: bool = False,
        max_parse_bytes: int | None = _DEFAULT_MAX_PARSE_BYTES,
    ) -> None:
        self._coerce = coerce_unknown_to_str
        self._max_parse_bytes = max_parse_bytes

    def _check_size(self, data: bytes) -> None:
        if self._max_parse_bytes is not None and len(data) > self._max_parse_bytes:
            raise ValueError(f"JSON input size {len(data)} exceeds max_parse_bytes={self._max_parse_bytes}")

    def parse(self, data: bytes, content_type: str | None = None) -> Any:
        self._check_size(data)
        return json.loads(data)

    def _default(self, data: Any) -> Any:
        if self._coerce:
            return str(data)
        raise TypeError(f"Object of type {type(data).__name__} is not JSON serializable")

    def serialize(self, data: Any) -> bytes:
        return json.dumps(data, default=self._default).encode("utf-8")

    @property
    def content_type(self) -> str:
        return "application/json"


class PydanticDecoder:
    """Decoder that uses Pydantic model_validate for decoding."""

    def decode(self, data: Any, target_type: type) -> Any:
        if hasattr(target_type, "model_validate") and isinstance(data, dict):
            return target_type.model_validate(data)
        return data

    def encode(self, data: Any) -> Any:
        if hasattr(data, "model_dump"):
            return data.model_dump()
        return data


class DataclassDecoder:
    """Decoder for stdlib dataclasses.

    H14 — no type validation or coercion: unlike ``PydanticDecoder``, this
    does NOT check field types. A field declared ``qty: int`` silently
    receives whatever JSON type was actually present (e.g. the string
    ``"3"``) if the producer sent the wrong type — stdlib dataclasses
    perform no runtime type checking on construction. **Use
    ``PydanticDecoder`` (or a msgspec-based decoder) instead for untrusted
    input where wrong-typed fields must be rejected.**

    Unknown keys in the incoming dict (not a declared field) are silently
    dropped rather than raising ``TypeError`` — this keeps a producer that
    adds a new field (forward-compatible) from turning every message into a
    decode failure (misclassified PERMANENT, straight to the DLQ) until
    every consumer is upgraded. A genuinely wrong shape (a missing required
    field, etc.) still raises — as a ``TypeError`` naming the target
    dataclass, not a bare constructor traceback.
    """

    def decode(self, data: Any, target_type: type) -> Any:
        import dataclasses

        if dataclasses.is_dataclass(target_type) and isinstance(data, dict):
            known_fields = {f.name for f in dataclasses.fields(target_type)}
            filtered = {k: v for k, v in data.items() if k in known_fields}
            try:
                return target_type(**filtered)
            except TypeError as exc:
                raise TypeError(f"Cannot decode into {target_type.__name__}: {exc}") from exc
        return data

    def encode(self, data: Any) -> Any:
        import dataclasses

        if dataclasses.is_dataclass(data) and not isinstance(data, type):
            return dataclasses.asdict(data)
        return data


class RawDecoder:
    """Pass-through decoder — no transformation."""

    def decode(self, data: Any, target_type: type) -> Any:
        return data

    def encode(self, data: Any) -> Any:
        return data
