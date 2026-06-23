"""Context, Header, and Path markers + ContextRepo."""

from __future__ import annotations

import threading
from typing import Any


class Context:
    """Marker for context value injection.

    Usage:
        @broker.subscriber(queue="orders")
        async def handle(
            order: Order,
            app_name: Annotated[str, Context("app")],
        ) -> None:
            ...
    """

    def __init__(self, key: str) -> None:
        self.key = key

    def __repr__(self) -> str:
        return f"Context({self.key!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Context):
            return NotImplemented
        return self.key == other.key

    def __hash__(self) -> int:
        return hash(("Context", self.key))


class Header:
    """Marker for AMQP header extraction.

    Usage:
        @broker.subscriber(queue="orders")
        async def handle(
            order: Order,
            tenant: Annotated[str, Header("x-tenant")],
        ) -> None:
            ...
    """

    def __init__(self, name: str) -> None:
        self.name = name

    def __repr__(self) -> str:
        return f"Header({self.name!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Header):
            return NotImplemented
        return self.name == other.name

    def __hash__(self) -> int:
        return hash(("Header", self.name))


class Path:
    """Marker for topic wildcard segment extraction.

    Usage:
        @broker.subscriber(queue="events", routing_key="events.*.#")
        async def handle(
            event: Event,
            level: Annotated[str, Path("level")],
        ) -> None:
            ...
    """

    def __init__(self, segment: str) -> None:
        self.segment = segment

    def __repr__(self) -> str:
        return f"Path({self.segment!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Path):
            return NotImplemented
        return self.segment == other.segment

    def __hash__(self) -> int:
        return hash(("Path", self.segment))


class ContextRepo:
    """Thread-safe context repository for global and local values.

    Global values are shared across all messages.
    Local values are per-thread (thread-local storage).
    """

    def __init__(self) -> None:
        self._global: dict[str, Any] = {}
        self._local = threading.local()

    def set_global(self, key: str, value: Any) -> None:
        """Set a global context value (shared across all messages)."""
        self._global[key] = value

    def set_local(self, key: str, value: Any) -> None:
        """Set a thread-local context value (per-message in sync transport)."""
        if not hasattr(self._local, "store"):
            self._local.store = {}
        self._local.store[key] = value

    def get(self, key: str, default: Any = None) -> Any:
        """Get a context value. Local overrides global."""
        # Check local first
        if hasattr(self._local, "store") and key in self._local.store:
            return self._local.store[key]
        # Then global
        return self._global.get(key, default)

    def clear_local(self) -> None:
        """Clear thread-local context (called after each message)."""
        if hasattr(self._local, "store"):
            self._local.store.clear()

    def has(self, key: str) -> bool:
        """Check if a key exists in either local or global context."""
        if hasattr(self._local, "store") and key in self._local.store:
            return True
        return key in self._global
