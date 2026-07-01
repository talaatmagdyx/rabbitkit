"""Context, Header, and Path markers + ContextRepo."""

from __future__ import annotations

import contextvars
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
    """Context repository for global and per-request values.

    Global values are shared across all messages (thread-safe via a lock).
    Local values use ``contextvars.ContextVar`` for correct isolation across
    both sync threads AND async coroutines on the same event loop —
    ``threading.local()`` would bleed context between concurrent coroutines
    sharing one OS thread in an async transport.
    """

    def __init__(self) -> None:
        self._global: dict[str, Any] = {}
        self._local: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar("rabbitkit_local_ctx")

    def set_global(self, key: str, value: Any) -> None:
        """Set a global context value (shared across all messages)."""
        self._global[key] = value

    def set_local(self, key: str, value: Any) -> None:
        """Set a per-request context value.

        Uses ``ContextVar.set`` with an immutable copy so that each
        coroutine/task gets its own isolated snapshot (contextvars
        copy-on-write semantics).
        """
        try:
            current = self._local.get()
        except LookupError:
            current = {}
        self._local.set({**current, key: value})

    def get(self, key: str, default: Any = None) -> Any:
        """Get a context value. Local overrides global."""
        try:
            local = self._local.get()
        except LookupError:
            local = {}
        if key in local:
            return local[key]
        return self._global.get(key, default)

    def clear_local(self) -> None:
        """Clear per-request context (called after each message)."""
        self._local.set({})

    def has(self, key: str) -> bool:
        """Check if a key exists in either local or global context."""
        try:
            return key in self._local.get() or key in self._global
        except LookupError:
            return key in self._global
