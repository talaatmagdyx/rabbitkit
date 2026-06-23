"""Depends marker — dependency injection for handlers."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any


class Depends:
    """Marker for dependency injection.

    Usage:
        async def get_db() -> Session:
            return Session()

        @broker.subscriber(queue="orders")
        async def handle(
            order: Order,
            db: Annotated[Session, Depends(get_db)],
        ) -> None:
            ...

    Per-message lifetime only (0.1.0). Generator dependencies deferred to 0.2.0.
    """

    def __init__(self, dependency: Callable[..., Any], *, use_cache: bool = True) -> None:
        self.dependency = dependency
        self.use_cache = use_cache

    def __repr__(self) -> str:
        return f"Depends({self.dependency.__qualname__}, use_cache={self.use_cache})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Depends):
            return NotImplemented
        return self.dependency is other.dependency and self.use_cache == other.use_cache

    def __hash__(self) -> int:
        return hash((id(self.dependency), self.use_cache))
