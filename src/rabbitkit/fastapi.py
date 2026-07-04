"""FastAPI integration — lifespan context manager for rabbitkit.

Thin module that wires broker startup/shutdown to FastAPI's lifespan.

Usage::

    from fastapi import FastAPI
    from rabbitkit.fastapi import rabbitkit_lifespan

    app = FastAPI(lifespan=rabbitkit_lifespan(broker=broker, rabbit_app=rabbit_app))

Or as a decorator-style::

    @asynccontextmanager
    async def lifespan(app):
        async with rabbitkit_lifespan(broker=broker):
            yield

    app = FastAPI(lifespan=lifespan)
"""

from __future__ import annotations

import asyncio
import inspect
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

logger = logging.getLogger(__name__)


@asynccontextmanager
async def rabbitkit_lifespan(
    app: Any = None,
    *,
    broker: Any | None = None,
    rabbit_app: Any | None = None,
) -> AsyncIterator[None]:
    """Async context manager that starts/stops rabbitkit components.

    Suitable for use as FastAPI's ``lifespan`` parameter or standalone.

    Start order: rabbit_app.start_async() → broker.start() / broker.start_async()
    Stop order (in finally): broker.stop() / broker.stop_async() → rabbit_app.stop_async()

    Duck-types sync vs async: if the method is a coroutine, it is awaited.

    Args:
        app: FastAPI app instance (passed by FastAPI lifespan protocol, may be None).
        broker: Optional rabbitkit broker (SyncBroker or AsyncBroker).
        rabbit_app: Optional RabbitApp for lifecycle hooks.
    """
    try:
        # Start rabbit_app first (startup hooks)
        if rabbit_app is not None:
            if hasattr(rabbit_app, "start_async"):
                await rabbit_app.start_async()
            elif hasattr(rabbit_app, "start"):
                result = rabbit_app.start()
                if asyncio.iscoroutine(result):
                    await result

        # Start broker
        if broker is not None:
            if inspect.iscoroutinefunction(getattr(broker, "start", None)):
                await broker.start()
            elif hasattr(broker, "start"):
                broker.start()

        logger.info("rabbitkit lifespan started")
        yield

    finally:
        # Stop broker first
        if broker is not None:
            if inspect.iscoroutinefunction(getattr(broker, "stop", None)):
                await broker.stop()
            elif hasattr(broker, "stop"):
                broker.stop()

        # Stop rabbit_app (shutdown hooks)
        if rabbit_app is not None:
            if hasattr(rabbit_app, "stop_async"):
                await rabbit_app.stop_async()
            elif hasattr(rabbit_app, "stop"):
                result = rabbit_app.stop()
                if asyncio.iscoroutine(result):
                    await result

        logger.info("rabbitkit lifespan stopped")
