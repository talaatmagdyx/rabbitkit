"""JSON serializer with Pydantic V2 support."""

from __future__ import annotations

import json
from dataclasses import asdict, fields, is_dataclass
from typing import Any


class JSONSerializer:
    """Default JSON serializer.

    Supports:
    - dict/list → json.dumps/loads
    - Pydantic V2 models → model_validate_json/model_dump_json
    - dataclasses → asdict → json.dumps
    - str → encode to UTF-8
    - bytes → pass through

    By default the serializer **raises** on objects ``json`` cannot represent
    (e.g. ``datetime``, ``Decimal``) instead of silently coercing them via
    ``str()``. Pass ``coerce_unknown_to_str=True`` to restore the legacy
    ``default=str`` coercion behaviour.

    H14 — decoding into a stdlib dataclass does **no type validation or
    coercion**: a field declared ``qty: int`` silently receives whatever
    JSON type was actually present if the producer sent the wrong type.
    Unknown keys in the payload are dropped rather than raising. Use a
    Pydantic model as ``target_type`` (or a msgspec-based serializer)
    instead of a stdlib dataclass for untrusted input where wrong-typed
    fields must be rejected.
    """

    #: M7: sane non-None default (64 MiB, matching
    #: ``CompressionMiddleware``'s ``max_decompressed_size`` default) so an
    #: uncompressed body is bounded out of the box. Pass ``max_parse_bytes=None``
    #: to opt out (unbounded) if you've already sized this elsewhere.
    _DEFAULT_MAX_PARSE_BYTES = 64 * 1024 * 1024

    def __init__(
        self,
        *,
        coerce_unknown_to_str: bool = False,
        max_parse_bytes: int | None = _DEFAULT_MAX_PARSE_BYTES,
    ) -> None:
        self._coerce = coerce_unknown_to_str
        # Defense-in-depth cap on the input size before json.loads (M7) — the
        # compression middleware already caps decompressed output; this
        # bounds the case where compression is off and a large body arrives
        # directly. Defaults to a sane non-None value rather than "off".
        self._max_parse_bytes = max_parse_bytes

    def _check_size(self, data: bytes) -> None:
        if self._max_parse_bytes is not None and len(data) > self._max_parse_bytes:
            raise ValueError(f"JSON input size {len(data)} exceeds max_parse_bytes={self._max_parse_bytes}")

    @property
    def content_type(self) -> str:
        return "application/json"

    def _default(self, obj: Any) -> Any:
        if self._coerce:
            return str(obj)
        raise TypeError(f"Object of type {type(obj).__name__} is not JSON serializable")

    def encode(self, data: Any) -> bytes:
        """Serialize data to JSON bytes."""
        if isinstance(data, bytes):
            return data
        if isinstance(data, str):
            return data.encode("utf-8")

        # Pydantic V2 model
        if hasattr(data, "model_dump_json"):
            result = data.model_dump_json()
            return result.encode("utf-8") if isinstance(result, str) else result

        # Dataclass
        if is_dataclass(data) and not isinstance(data, type):
            return json.dumps(asdict(data), default=self._default).encode("utf-8")

        # Dict, list, or other JSON-serializable
        return json.dumps(data, default=self._default).encode("utf-8")

    def decode(self, data: bytes, target_type: type) -> Any:
        """Deserialize JSON bytes to target type."""
        self._check_size(data)
        if target_type is bytes:
            return data
        if target_type is str:
            return data.decode("utf-8")
        if target_type is dict:
            return json.loads(data)
        if target_type is list:
            return json.loads(data)

        # Pydantic V2 model
        if hasattr(target_type, "model_validate_json"):
            return target_type.model_validate_json(data)

        # Pydantic V2 model via model_validate (dict input)
        if hasattr(target_type, "model_validate"):
            parsed = json.loads(data)
            return target_type.model_validate(parsed)

        # Dataclass (H14: no type validation/coercion -- see DataclassDecoder
        # in serialization/pipeline.py for the full contract this mirrors.
        # Unknown keys are dropped rather than raising; a genuinely wrong
        # shape raises TypeError naming the target dataclass.)
        if is_dataclass(target_type):
            parsed = json.loads(data)
            if isinstance(parsed, dict):
                known_fields = {f.name for f in fields(target_type)}
                filtered = {k: v for k, v in parsed.items() if k in known_fields}
                try:
                    return target_type(**filtered)
                except TypeError as exc:
                    raise TypeError(f"Cannot decode into {target_type.__name__}: {exc}") from exc
            return parsed

        # Fallback: json.loads
        return json.loads(data)
