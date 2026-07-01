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
            self._decoders: dict[Any, Any] = {}  # cached decoders per target_type (perf)
        except ImportError as e:  # pragma: no cover
            raise ImportError(  # pragma: no cover
                "msgspec is required for MsgspecSerializer. Install it with: pip install rabbitkit[msgspec]"
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
        if target_type is dict or target_type is list:
            return self._msgspec.json.decode(data)

        # Generic alias (dict[str, Any], list[int], …) — origin is dict/list
        origin = getattr(target_type, "__origin__", None)
        if origin is dict or origin is list:
            decoder = self._decoders.get(target_type)
            if decoder is None:
                try:
                    decoder = self._msgspec.json.Decoder(type=target_type)
                except Exception:
                    return self._msgspec.json.decode(data)
                self._decoders[target_type] = decoder
            return decoder.decode(data)

        # Pydantic V2 model — model_validate_json is faster than json.loads + model_validate
        if hasattr(target_type, "model_validate_json"):
            return target_type.model_validate_json(data)

        # msgspec Struct — fastest path for msgspec-native types
        try:
            is_struct = issubclass(target_type, self._msgspec.Struct)
        except TypeError:
            is_struct = False
        if is_struct:
            return self._msgspec.json.decode(data, type=target_type)

        # General typed decoder — cached per target_type (Decoder(type=T) codegens
        # a converter; rebuilding per call defeats msgspec's performance advantage).
        decoder = self._decoders.get(target_type)
        if decoder is None:
            try:
                decoder = self._msgspec.json.Decoder(type=target_type)
            except Exception:
                return self._msgspec.json.decode(data)
            self._decoders[target_type] = decoder
        return decoder.decode(data)
