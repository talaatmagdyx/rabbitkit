"""Monitoring dashboard ASGI application.

Provides a lightweight, dependency-free HTTP dashboard for monitoring
a running rabbitkit broker.  Built on **Starlette** (optional dependency).

Requires: ``pip install rabbitkit[dashboard]``

Endpoints
---------
GET /               — HTML dashboard with route table and health status
GET /api/health     — JSON health snapshot (status, connected, consumer_count, …)
GET /api/routes     — JSON array of all registered routes

Quick start
-----------
Mount as a standalone ASGI app with Uvicorn::

    # myapp/dashboard.py
    from rabbitkit.dashboard import create_dashboard_app
    from myapp.main import broker      # your AsyncBroker or SyncBroker

    app = create_dashboard_app(broker)

    # Run separately:
    # uvicorn myapp.dashboard:app --host 0.0.0.0 --port 8080

CLI shortcut::

    rabbitkit dashboard myapp.main:broker --port 8080

Mount inside an existing FastAPI / Starlette app::

    from fastapi import FastAPI
    from starlette.routing import Mount
    from rabbitkit.dashboard import create_dashboard_app

    api = FastAPI()
    dashboard = create_dashboard_app(broker)
    api.mount("/rabbit", dashboard)

With Management API integration (adds live queue stats)::

    from rabbitkit.management import RabbitManagementClient, ManagementConfig
    from rabbitkit.dashboard import create_dashboard_app

    mgmt = RabbitManagementClient(
        ManagementConfig(url="http://rabbitmq:15672", username="admin", password="secret")
    )

With MetricsCollector (adds throughput / latency metrics from MetricsMiddleware)::

    from rabbitkit.middleware.metrics import MetricsCollector
    from rabbitkit.dashboard import create_dashboard_app

    collector = MetricsCollector()

Health status values
--------------------
``GET /api/health`` returns:

    {
      "status":         "healthy" | "degraded" | "unhealthy",
      "started":        true,
      "connected":      true,
      "consumer_count": 3,
      "route_count":    3
    }
"""

from __future__ import annotations

import logging
from html import escape
from typing import Any

logger = logging.getLogger(__name__)


def create_dashboard_app(
    broker: Any,
    *,
    auth_token: str | None = None,
) -> Any:
    """Create an ASGI dashboard application.

    SECURITY: by default this app has NO authentication and exposes broker
    topology (queue/exchange names, consumer counts). Mount it behind authn
    (OIDC/reverse proxy) and restrict it to an internal network — never expose
    it publicly.

    For a lightweight built-in guard, pass ``auth_token``: when set, every
    route requires an ``Authorization: Bearer <auth_token>`` header and
    returns ``401`` otherwise. When unset (default), all requests pass through
    and a startup warning is logged reminding you not to expose the dashboard
    publicly.

    Args:
        broker: A rabbitkit broker instance (SyncBroker or AsyncBroker).
        auth_token: Optional bearer token. When set, all routes require
            ``Authorization: Bearer <auth_token>``. When None, the dashboard
            runs unauthenticated (a startup warning is emitted).

    Returns:
        A Starlette application.

    Raises:
        ImportError: If starlette is not installed.
    """
    try:
        from starlette.applications import Starlette
        from starlette.middleware.base import BaseHTTPMiddleware
        from starlette.requests import Request  # noqa: TC002  # lazy optional import
        from starlette.responses import HTMLResponse, JSONResponse, PlainTextResponse, Response
        from starlette.routing import Route
    except ImportError:  # pragma: no cover
        raise ImportError(  # pragma: no cover
            "Dashboard requires starlette. Install with: pip install rabbitkit[dashboard]"
        ) from None

    from rabbitkit.health import broker_health_check

    if auth_token is None:
        logger.warning("Dashboard running WITHOUT authentication — do not expose publicly")

    class _BearerAuthMiddleware(BaseHTTPMiddleware):
        async def dispatch(self, request: Request, call_next: Any) -> Response:
            auth = request.headers.get("Authorization", "")
            if auth != f"Bearer {auth_token}":
                return PlainTextResponse("Unauthorized", status_code=401)
            return await call_next(request)  # type: ignore[no-any-return]

    async def index(request: Any) -> Any:
        routes_count = len(broker.routes)
        health = broker_health_check(broker)
        html = f"""<!DOCTYPE html>
<html><head><title>rabbitkit Dashboard</title>
<style>
body {{ font-family: system-ui, sans-serif; max-width: 900px; margin: 2rem auto; padding: 0 1rem; }}
h1 {{ color: #333; }}
table {{ border-collapse: collapse; width: 100%; margin: 1rem 0; }}
th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
th {{ background: #f5f5f5; }}
.healthy {{ color: green; }} .degraded {{ color: orange; }} .unhealthy {{ color: red; }}
</style></head>
<body>
<h1>rabbitkit Dashboard</h1>
<h2>Health: <span class="{health.status.value}">{health.status.value.upper()}</span></h2>
<p>Routes: {routes_count} | Connected: {health.connected} | Consumers: {health.consumer_count}</p>
<h2>Routes</h2>
<table><tr><th>Name</th><th>Queue</th><th>Exchange</th><th>Ack Policy</th></tr>"""
        for r in broker.routes:
            exchange = r.exchange.name if r.exchange else ""
            # escape() — queue/exchange/route names render into HTML; never trust them raw
            html += (
                f"<tr><td>{escape(r.name)}</td><td>{escape(r.queue.name)}</td>"
                f"<td>{escape(exchange)}</td><td>{escape(r.ack_policy.value)}</td></tr>"
            )
        html += "</table></body></html>"
        return HTMLResponse(html)

    async def api_health(request: Any) -> Any:
        health = broker_health_check(broker)
        return JSONResponse(
            {
                "status": health.status.value,
                "started": health.started,
                "connected": health.connected,
                "consumer_count": health.consumer_count,
                "route_count": health.route_count,
            }
        )

    async def api_routes(request: Any) -> Any:
        routes = []
        for r in broker.routes:
            routes.append(
                {
                    "name": r.name,
                    "queue": r.queue.name,
                    "exchange": r.exchange.name if r.exchange else "",
                    "ack_policy": r.ack_policy.value,
                    "tags": sorted(r.tags) if r.tags else [],
                    "description": r.description,
                }
            )
        return JSONResponse(routes)

    app = Starlette(
        routes=[
            Route("/", index),
            Route("/api/health", api_health),
            Route("/api/routes", api_routes),
        ],
    )
    if auth_token is not None:
        app.add_middleware(_BearerAuthMiddleware)
    return app
