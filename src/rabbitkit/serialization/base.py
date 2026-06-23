"""Serializer protocol — pluggable serialization contract."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class Serializer(Protocol):
    """Pluggable serializer protocol.

    Implementations must provide encode() and decode() methods.
    rabbitkit ships with JSONSerializer and MsgspecSerializer.
    """

    def encode(self, data: Any) -> bytes:
        """Serialize data to bytes."""
        ...

    def decode(self, data: bytes, target_type: type) -> Any:
        """Deserialize bytes to target type."""
        ...

    @property
    def content_type(self) -> str:
        """MIME content type for this serializer."""
        ...
