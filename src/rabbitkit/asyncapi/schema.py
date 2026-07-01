"""JSON Schema extraction from Python type annotations."""

from __future__ import annotations

import dataclasses
import inspect
from typing import Any, get_type_hints

from rabbitkit.core.message import is_rabbit_message_annotation


def get_handler_body_type(handler: Any) -> type | None:
    """Extract the body parameter type from a handler signature.

    Returns the first non-RabbitMessage, non-Annotated parameter type.
    Returns None if no suitable parameter found or type is bytes.
    """
    try:
        sig = inspect.signature(handler)
    except (ValueError, TypeError):
        return None

    # Resolve string annotations (from __future__ import annotations)
    try:
        hints = get_type_hints(handler)
    except Exception:
        hints = {}

    for name, param in sig.parameters.items():
        # Prefer resolved type hints over raw annotations
        ann = hints.get(name, param.annotation)
        if ann is inspect.Parameter.empty:
            continue
        if is_rabbit_message_annotation(ann):
            continue
        # Skip Annotated types (DI markers)
        if getattr(ann, "__metadata__", None) is not None:
            continue
        if ann is bytes:
            return None
        return ann  # type: ignore[no-any-return]
    return None


def extract_json_schema(type_hint: type | None) -> dict[str, Any]:
    """Extract JSON Schema from a Python type hint.

    Supports:
    - None -> empty schema
    - Pydantic V2 models -> model_json_schema()
    - dataclasses -> field inspection
    - Primitives (str, int, float, bool) -> {"type": "..."}
    - bytes -> {"type": "string", "contentEncoding": "base64"}
    """
    if type_hint is None:
        return {}

    # Pydantic V2 model
    if hasattr(type_hint, "model_json_schema"):
        return type_hint.model_json_schema()  # type: ignore[no-any-return]

    # dataclass
    if dataclasses.is_dataclass(type_hint) and isinstance(type_hint, type):
        properties: dict[str, Any] = {}
        required: list[str] = []
        for f in dataclasses.fields(type_hint):
            properties[f.name] = _python_type_to_schema(f.type)
            if f.default is dataclasses.MISSING and f.default_factory is dataclasses.MISSING:
                required.append(f.name)
        schema: dict[str, Any] = {"type": "object", "properties": properties}
        if required:
            schema["required"] = required
        return schema

    # Primitive types
    return _python_type_to_schema(type_hint)


def _python_type_to_schema(python_type: Any) -> dict[str, Any]:
    """Convert a Python type to a JSON Schema type."""
    type_map: dict[type, dict[str, str]] = {
        str: {"type": "string"},
        int: {"type": "integer"},
        float: {"type": "number"},
        bool: {"type": "boolean"},
        bytes: {"type": "string", "contentEncoding": "base64"},
        dict: {"type": "object"},
        list: {"type": "array"},
    }
    if isinstance(python_type, type) and python_type in type_map:
        return type_map[python_type]
    if isinstance(python_type, str):
        # String annotation — best effort
        lower = python_type.lower()
        for t, schema in type_map.items():
            if t.__name__.lower() == lower:
                return schema
    return {"type": "object"}
