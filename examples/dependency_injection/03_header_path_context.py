"""Dependency Injection: Header(), Path(), Context() markers.

Extract values from message headers, topic wildcard segments,
and application-level context without manual inspection code.

Run:
    python examples/dependency_injection/03_header_path_context.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
from typing import Annotated

from rabbitkit import RabbitConfig, MessageEnvelope
from rabbitkit.async_ import AsyncBroker
from rabbitkit.di.context import Header, Path, Context

broker = AsyncBroker(RabbitConfig())


# ── 1. Header() — extract AMQP message header ────────────────────────────────

@broker.subscriber(queue="header-demo")
async def handle_with_headers(
    body: bytes,
    tenant: Annotated[str, Header("x-tenant")],
    priority: Annotated[str, Header("x-priority", default="normal")],
    request_id: Annotated[str | None, Header("x-request-id", default=None)],
) -> None:
    """Headers are extracted and type-coerced automatically."""
    print(f"[header] tenant={tenant!r}, priority={priority!r}, req_id={request_id!r}")
    print(f"[header] body: {body.decode()}")


# ── 2. Path() — extract topic wildcard segments ───────────────────────────────
# When routing_key="events.{region}.{service}", Path("region") gives
# the matched value for that segment.

@broker.subscriber(
    queue="path-demo",
    exchange="events",
    routing_key="events.*.*",   # two wildcard segments
)
async def handle_with_path(
    body: bytes,
    region: Annotated[str, Path("region")],   # first wildcard
    service: Annotated[str, Path("service")], # second wildcard
) -> None:
    print(f"[path] region={region!r}, service={service!r}")
    print(f"[path] body: {body.decode()}")


# ── 3. Context() — extract application-level context values ──────────────────
# Context values are registered in context_repo (a dict-like store).
# Useful for injecting app-wide settings (feature flags, tenant configs, etc.)

# In a real app you'd use an actual ContextRepository:
# from rabbitkit.di.context import InMemoryContextRepository
# context_repo = InMemoryContextRepository({"app_name": "order-service", "version": "2.1"})
# broker = AsyncBroker(config, context_repo=context_repo)

@broker.subscriber(queue="context-demo")
async def handle_with_context(
    body: bytes,
    app_name: Annotated[str, Context("app_name", default="unknown")],
) -> None:
    print(f"[context] app={app_name!r}, body={body.decode()!r}")


# ── 4. Combining all three ───────────────────────────────────────────────────

@broker.subscriber(
    queue="combined-demo",
    exchange="audit",
    routing_key="audit.*",
)
async def handle_combined(
    body: bytes,
    tenant: Annotated[str, Header("x-tenant", default="default")],
    action: Annotated[str, Path("action")],
) -> None:
    print(f"[combined] tenant={tenant!r}, action={action!r}, body={body.decode()!r}")


async def main() -> None:
    await broker.start()

    # Header example
    await broker.publish(MessageEnvelope(
        routing_key="header-demo",
        body=b'{"event": "user.login"}',
        headers={
            "x-tenant": "acme",
            "x-priority": "high",
            "x-request-id": "req-abc-123",
        },
    ))

    # Missing optional header — uses default
    await broker.publish(MessageEnvelope(
        routing_key="header-demo",
        body=b'{"event": "ping"}',
        headers={"x-tenant": "beta"},  # x-priority not set → uses "normal"
    ))

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
