"""Advanced: Monitoring dashboard — Starlette ASGI app.

Provides real-time broker health and route inspection via HTTP.
Requires: pip install "rabbitkit[async,dashboard]"

Run:
    python examples/advanced/07_monitoring_dashboard.py

Then open: http://localhost:8080

Requirements:
    pip install "rabbitkit[async,dashboard]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker

try:
    import uvicorn

    from rabbitkit.dashboard import create_dashboard_app
    DASHBOARD_AVAILABLE = True
except ImportError:
    DASHBOARD_AVAILABLE = False
    print("Dashboard not available — run: pip install 'rabbitkit[dashboard]'")


broker = AsyncBroker(RabbitConfig())


# ── Register some routes so the dashboard has something to show ───────────────

@broker.subscriber(queue="orders", tags={"orders", "critical"}, description="Process customer orders")
async def handle_order(body: bytes) -> None:
    pass

@broker.subscriber(queue="notifications", tags={"notifications"}, description="Send email/SMS notifications")
async def handle_notification(body: bytes) -> None:
    pass

@broker.subscriber(queue="analytics", tags={"analytics"}, description="Record analytics events")
async def handle_analytics(body: bytes) -> None:
    pass


async def main() -> None:
    if not DASHBOARD_AVAILABLE:
        return

    await broker.start()
    print(f"Broker started with {len(broker.routes)} routes")

    # ── Basic dashboard ───────────────────────────────────────────────────────
    dashboard_app = create_dashboard_app(broker)

    # ── Dashboard + Management API (optional, shows live queue stats) ─────────
    # from rabbitkit.management import RabbitManagementClient, ManagementConfig
    # mgmt = RabbitManagementClient(ManagementConfig(
    #     url="http://localhost:15672",
    #     username="guest",
    #     password="guest",
    # ))
    # dashboard_app = create_dashboard_app(broker, management_client=mgmt)

    # ── Run standalone on port 8080 ───────────────────────────────────────────
    print("\nDashboard available at: http://localhost:8080")
    print("  GET /         — HTML dashboard")
    print("  GET /api/health  — JSON health")
    print("  GET /api/routes  — JSON routes")
    print("\nPress Ctrl+C to stop.\n")

    server = uvicorn.Server(uvicorn.Config(
        app=dashboard_app,
        host="0.0.0.0",
        port=8080,
        log_level="warning",
    ))

    try:
        await server.serve()
    except KeyboardInterrupt:
        pass
    finally:
        await broker.stop()


# ── Mount inside FastAPI ──────────────────────────────────────────────────────
#
# from fastapi import FastAPI
# from rabbitkit.dashboard import create_dashboard_app
#
# api = FastAPI()
# rabbit_dashboard = create_dashboard_app(broker)
#
# # Mount at /rabbit — access via http://localhost:8000/rabbit
# api.mount("/rabbit", rabbit_dashboard)
#
# # FastAPI lifespan handles broker start/stop:
# from rabbitkit.fastapi import rabbitkit_lifespan
# api = FastAPI(lifespan=rabbitkit_lifespan(broker=broker))
# api.mount("/rabbit", create_dashboard_app(broker))


if __name__ == "__main__":
    asyncio.run(main())
