"""Business service + its DI factory.

In-memory for the example, but it models the real shape from docs §13:
idempotency check + business change as one atomic step. Swap the dict for a DB
transaction with an INSERT ... ON CONFLICT on processed_messages in production.

The factory ``get_order_service`` is MODULE-LEVEL on purpose: with
``from __future__ import annotations`` all annotations are lazy strings, so
``typing.get_type_hints`` must be able to resolve ``Depends(get_order_service)``
from module globals (see CLAUDE.md / docs §12).
"""

from __future__ import annotations

from .errors import DownstreamUnavailable, DuplicateOrder, InvalidTenant
from .models import OrderCreated


class OrderService:
    def __init__(self) -> None:
        self._orders: dict[str, OrderCreated] = {}
        self._processed: set[tuple[str, int]] = set()
        self.known_tenants: set[str] = {"t-1", "t-2"}
        # Toggle to simulate a downstream dependency outage (→ transient failures).
        self.downstream_up: bool = True

    def already_processed(self, order_id: str, event_version: int) -> bool:
        return (order_id, event_version) in self._processed

    def create_order(self, event: OrderCreated) -> None:
        if not self.downstream_up:
            # Transient: a dependency (e.g. payments) is unreachable right now.
            raise DownstreamUnavailable("payments service unavailable")
        if event.tenant_id not in self.known_tenants:
            raise InvalidTenant(event.tenant_id)  # permanent → DLQ
        if event.order_id in self._orders:
            raise DuplicateOrder(event.order_id)  # permanent → DLQ
        # Atomic in a real DB: business change + idempotency record together.
        self._orders[event.order_id] = event
        self._processed.add((event.order_id, event.event_version))

    def reset(self) -> None:
        """Test helper — restore a clean state between tests."""
        self._orders.clear()
        self._processed.clear()
        self.known_tenants = {"t-1", "t-2"}
        self.downstream_up = True


# Module-level singleton + factory (see docstring on why module-level matters).
_SERVICE = OrderService()


def get_order_service() -> OrderService:
    return _SERVICE
