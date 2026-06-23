"""FastAPI integration (docs §18/§30): lifespan-managed broker, health endpoint,
and the read-only ops dashboard mounted behind your auth/network policy.

FastAPI/Starlette are imported lazily inside ``create_app`` so importing this
module never requires them; call ``create_app`` to build the ASGI app.
"""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any


def create_app(broker: Any, management_client: Any | None = None) -> Any:
    from fastapi import FastAPI

    from rabbitkit.dashboard import create_dashboard_app
    from rabbitkit.fastapi import rabbitkit_lifespan
    from rabbitkit.health import broker_health_check_async

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> Any:
        async with rabbitkit_lifespan(broker=broker):  # starts/stops the broker with the app
            yield

    app = FastAPI(title="order-service", lifespan=lifespan)

    @app.get("/health/ready")
    async def ready() -> dict[str, Any]:
        r = await broker_health_check_async(broker)
        return {"status": r.status, "connected": r.connected, "consumers": r.consumer_count}

    # Restrict in production: OIDC + internal network only (docs §27).
    app.mount("/_rabbit", create_dashboard_app(broker, management_client=management_client))
    return app
