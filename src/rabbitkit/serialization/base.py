"""Serializer protocol — pluggable serialization contract."""

from __future__ import annotations

from typing import Any, Protocol, TypeVar, runtime_checkable

T = TypeVar("T")


@runtime_checkable
class Serializer(Protocol[T]):
    """Pluggable serializer protocol.

    Generic in ``T`` so callers can pin the type flowing through
    ``decode(data, target_type) -> T`` — e.g. ``Serializer[Order]`` makes
    mypy verify ``decode`` returns ``Order``.

    Implementations must provide encode() and decode() methods.
    rabbitkit ships with JSONSerializer and MsgspecSerializer. The built-in
    implementations work with any ``T`` (they satisfy ``Serializer[Any]``
    structurally) because they use ``Any`` internally.
    """

    def encode(self, data: Any) -> bytes:
        """Serialize data to bytes."""
        ...

    def decode(self, data: bytes, target_type: type[T]) -> T:
        """Deserialize bytes to target type."""
        ...

    @property
    def content_type(self) -> str:
        """MIME content type for this serializer."""
        ...
