"""msgspec serializer — optional high-performance serialization.

Requires: pip install rabbitkit[msgspec]
"""

from __future__ import annotations

from typing import Any


class MsgspecSerializer:
    """High-performance serializer using msgspec.

    Requires msgspec to be installed (optional dependency).
    Falls back with clear error if not available.
    """

    def __init__(self) -> None:
        try:
            import msgspec

            self._msgspec = msgspec
            self._encoder = msgspec.json.Encoder()
            self._decoder = msgspec.json.Decoder()
        except ImportError as e:  # pragma: no cover
            raise ImportError(  # pragma: no cover
                "msgspec is required for MsgspecSerializer. "
                "Install it with: pip install rabbitkit[msgspec]"
            ) from e

    @property
    def content_type(self) -> str:
        return "application/json"

    def encode(self, data: Any) -> bytes:
        """Serialize data to JSON bytes using msgspec."""
        if isinstance(data, bytes):
            return data
        if isinstance(data, str):
            return data.encode("utf-8")

        # msgspec Struct
        if isinstance(data, self._msgspec.Struct):
            return self._msgspec.json.encode(data)

        # General encoding
        return self._encoder.encode(data)

    def decode(self, data: bytes, target_type: type) -> Any:
        """Deserialize JSON bytes using msgspec."""
        if target_type is bytes:
            return data
        if target_type is str:
            return data.decode("utf-8")

        # msgspec Struct
        if issubclass(target_type, self._msgspec.Struct):
            return self._msgspec.json.decode(data, type=target_type)

        # General decoding
        decoder = self._msgspec.json.Decoder(type=target_type)
        return decoder.decode(data)
