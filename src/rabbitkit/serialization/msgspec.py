"""msgspec serializer — optional high-performance serialization.

Requires: pip install rabbitkit[msgspec]
"""

from __future__ import annotations

from typing import Any


class MsgspecSerializer:
    """High-performance serializer using msgspec.

    Requires msgspec to be installed (optional dependency).
    Falls back with clear error if not available.

    M7: caps the input size before decoding (64 MiB by default, matching
    ``CompressionMiddleware``'s ``max_decompressed_size`` default) — without
    it, a large uncompressed body is fully materialized with no bound.
    Pass ``max_parse_bytes=None`` to opt out.
    """

    #: M7: see JSONSerializer's identical default for the rationale.
    _DEFAULT_MAX_PARSE_BYTES = 64 * 1024 * 1024

    def __init__(self, *, max_parse_bytes: int | None = _DEFAULT_MAX_PARSE_BYTES) -> None:
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
        self._max_parse_bytes = max_parse_bytes

    def _check_size(self, data: bytes) -> None:
        if self._max_parse_bytes is not None and len(data) > self._max_parse_bytes:
            raise ValueError(f"JSON input size {len(data)} exceeds max_parse_bytes={self._max_parse_bytes}")

    @property
    def content_type(self) -> str:
        """Advisory only (M10): this is the ``content_type`` used when
        *publishing* (set on the outgoing AMQP message property). It is
        **not** verified against an incoming message's declared
        ``content_type`` on :meth:`decode` — decode is driven solely by the
        handler's declared parameter type, matching every other built-in
        serializer in this codebase (none of them negotiate/verify
        content-type on the consume side). If a message's actual body
        doesn't match what this serializer expects (e.g. it's not JSON at
        all), :meth:`decode` raises with a message naming both the target
        type and a content-type-mismatch hint, rather than a raw
        ``msgspec.DecodeError``.
        """
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
        """Deserialize JSON bytes using msgspec.

        M10: raises with a clearer message (naming the target type and
        hinting at a content-type mismatch) instead of a raw
        ``msgspec.DecodeError`` if the body isn't valid JSON for
        *target_type* — most commonly caused by a message whose actual
        content_type doesn't match what this serializer expects (see
        :attr:`content_type`'s docstring — this is never verified upfront).
        """
        try:
            return self._decode(data, target_type)
        except self._msgspec.DecodeError as exc:
            raise ValueError(
                f"Failed to decode message body as JSON for target type {target_type!r}: {exc}. "
                "If the message's actual content_type doesn't match what this serializer expects "
                "(application/json), that's the likely cause — MsgspecSerializer does not verify "
                "content_type before decoding."
            ) from exc

    def _decode(self, data: bytes, target_type: type) -> Any:
        self._check_size(data)
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
