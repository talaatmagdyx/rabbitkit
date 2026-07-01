"""Serialization module — pluggable encode/decode."""

from rabbitkit.serialization.json import JSONSerializer
from rabbitkit.serialization.msgspec import MsgspecSerializer
from rabbitkit.serialization.pipeline import (
    DataclassDecoder,
    JsonParser,
    MessageDecoder,
    MessageParser,
    PydanticDecoder,
    RawDecoder,
    SerializationPipeline,
)

__all__ = [
    "DataclassDecoder",
    "JSONSerializer",
    "JsonParser",
    "MessageDecoder",
    "MessageParser",
    "MsgspecSerializer",
    "PydanticDecoder",
    "RawDecoder",
    "SerializationPipeline",
]
