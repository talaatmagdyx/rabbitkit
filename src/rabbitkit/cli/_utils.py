"""CLI utilities — broker loading and path parsing."""

from __future__ import annotations

import importlib
from typing import Any


def parse_app_path(app_path: str) -> tuple[str, str]:
    """Split 'module.path:attr' into (module_path, attr_name).

    Raises ValueError if format is invalid.
    """
    if ":" not in app_path:
        raise ValueError(
            f"Invalid app path '{app_path}'. Expected format: 'module.path:broker_attr'"
        )
    module_path, attr = app_path.rsplit(":", 1)
    return module_path, attr


def load_broker(app_path: str) -> Any:
    """Import module and return the broker attribute.

    Args:
        app_path: String like 'myapp.main:broker'

    Returns:
        The broker instance.

    Raises:
        ValueError: Invalid path format.
        ImportError: Module not found.
        AttributeError: Attribute not found in module.
    """
    module_path, attr = parse_app_path(app_path)
    module = importlib.import_module(module_path)
    return getattr(module, attr)
