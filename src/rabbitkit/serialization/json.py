"""JSON serializer with Pydantic V2 support."""

from __future__ import annotations

import json
from dataclasses import asdict, is_dataclass
from typing import Any


class JSONSerializer:
    """Default JSON serializer.

    Supports:
    - dict/list → json.dumps/loads
    - Pydantic V2 models → model_validate_json/model_dump_json
    - dataclasses → asdict → json.dumps
    - str → encode to UTF-8
    - bytes → pass through
    """

    @property
    def content_type(self) -> str:
        return "application/json"

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
            return json.dumps(asdict(data), default=str).encode("utf-8")

        # Dict, list, or other JSON-serializable
        return json.dumps(data, default=str).encode("utf-8")

    def decode(self, data: bytes, target_type: type) -> Any:
        """Deserialize JSON bytes to target type."""
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

        # Dataclass
        if is_dataclass(target_type):
            parsed = json.loads(data)
            if isinstance(parsed, dict):
                return target_type(**parsed)
            return parsed

        # Fallback: json.loads
        return json.loads(data)
