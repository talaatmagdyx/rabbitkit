"""Handler registration.

Exposed as a function so the SAME decorated handler runs on both a real broker
and the in-memory ``TestBroker`` (docs §25). The caller supplies the middleware
list (so retry can be wired with a real ``publish_async_fn`` — docs §0, truth #1/#2).

IMPORTANT: this module deliberately does NOT use ``from __future__ import
annotations``. The pipeline resolves the body type via ``inspect.signature`` and
reads ``param.annotation`` directly — under future-annotations that would be the
string ``"OrderCreated"`` (no ``model_validate``), so pydantic decoding would be
silently skipped and the handler would receive a raw dict. Keep annotations as
real objects here. (The shipped serialization examples follow the same rule.)
"""

from typing import Annotated, Any

from rabbitkit.core.types import AckPolicy
from rabbitkit.di.depends import Depends

from .models import OrderCreated
from .services import OrderService, get_order_service


# MODULE-LEVEL handler with REAL (non-stringized) annotations so the pipeline can
# read `event: OrderCreated` from inspect.signature and pydantic-decode the body.
async def handle_order_created(
    event: OrderCreated,  # body → validated pydantic model (serializer-decoded)
    svc: Annotated[OrderService, Depends(get_order_service)],
) -> None:
    # Fast-path idempotency; the DB record in create_order() is the real guard.
    if svc.already_processed(event.order_id, event.event_version):
        return
    svc.create_order(event)


def register_order_handlers(
    broker: Any,
    *,
    middlewares: list[Any] | None = None,
) -> None:
    """Register the order handler on a broker (real or TestBroker) with the given
    middleware pipeline. Same decorator path as production (docs §25)."""
    broker.subscriber(
        queue="orders.queue",
        exchange="orders.exchange",
        routing_key="orders.created",
        # NACK_ON_ERROR (not AUTO): terminal failures nack(requeue=False) → DLQ,
        # instead of AUTO's requeue=True hot-loop on exhausted-transient (docs §0/§8).
        ack_policy=AckPolicy.NACK_ON_ERROR,
        middlewares=middlewares or [],
        name="order_created",
        description="Create an order from an orders.created event.",
    )(handle_order_created)
