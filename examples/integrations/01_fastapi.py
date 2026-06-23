"""Integration: FastAPI + rabbitkit_lifespan.

rabbitkit_lifespan() wires broker start/stop into FastAPI's lifespan
context manager. Both sync and async brokers are supported.

Run:
    pip install "rabbitkit[async,fastapi]"
    uvicorn examples.integrations.01_fastapi:app --reload

Then test:
    curl http://localhost:8000/health
    curl -X POST http://localhost:8000/orders -H "Content-Type: application/json" -d '{"id":1,"item":"Widget"}'

Requirements:
    pip install "rabbitkit[async,fastapi]"
    RabbitMQ running on localhost:5672
"""

import json
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator

from rabbitkit import RabbitConfig, MessageEnvelope
from rabbitkit.async_ import AsyncBroker

try:
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import JSONResponse
    from rabbitkit.fastapi import rabbitkit_lifespan
    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False
    print("FastAPI not available — run: pip install 'rabbitkit[fastapi]'")

if not FASTAPI_AVAILABLE:
    raise SystemExit(0)

# ── Broker setup ──────────────────────────────────────────────────────────────
broker = AsyncBroker(RabbitConfig())

processed_orders: list[dict[str, Any]] = []


@broker.subscriber(queue="api-orders")
async def handle_order(body: bytes) -> None:
    """Process orders received from the API."""
    data = json.loads(body)
    processed_orders.append(data)
    print(f"[consumer] processed order #{data['id']}: {data.get('item')}")


# ── FastAPI app with rabbitkit lifespan ───────────────────────────────────────
app = FastAPI(
    title="Order Service",
    description="Example FastAPI app with rabbitkit integration",
    lifespan=rabbitkit_lifespan(broker=broker),
)


# ── API endpoints ─────────────────────────────────────────────────────────────

@app.get("/health")
async def health() -> dict[str, Any]:
    """Check broker connectivity."""
    from rabbitkit.health import broker_health_check
    health = broker_health_check(broker)
    return {
        "status": health.status.value,
        "connected": health.connected,
        "routes": health.route_count,
    }


@app.post("/orders")
async def create_order(order: dict[str, Any]) -> dict[str, str]:
    """Publish an order to the processing queue."""
    if "id" not in order:
        raise HTTPException(status_code=400, detail="order.id required")

    await broker.publish(MessageEnvelope(
        routing_key="api-orders",
        body=json.dumps(order).encode(),
        content_type="application/json",
    ))
    return {"status": "accepted", "order_id": str(order["id"])}


@app.get("/orders")
async def list_processed() -> dict[str, Any]:
    """List orders processed so far (in-memory, demo only)."""
    return {"count": len(processed_orders), "orders": processed_orders}


@app.get("/routes")
async def list_routes() -> dict[str, Any]:
    """List all registered broker routes."""
    return {
        "routes": [
            {
                "name": r.name,
                "queue": r.queue.name,
                "exchange": r.exchange.name if r.exchange else "",
            }
            for r in broker.routes
        ]
    }


# ── Advanced: RabbitApp integration ──────────────────────────────────────────
# Use RabbitApp for startup/shutdown hooks + state tracking:
#
# from rabbitkit import RabbitApp
# from rabbitkit.fastapi import rabbitkit_lifespan
#
# rabbit_app = RabbitApp(title="order-service")
#
# @rabbit_app.on_startup
# async def init_db():
#     await db.connect()
#
# @rabbit_app.on_shutdown
# async def close_db():
#     await db.disconnect()
#
# app = FastAPI(
#     lifespan=rabbitkit_lifespan(broker=broker, rabbit_app=rabbit_app)
# )

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("examples.integrations.01_fastapi:app", host="0.0.0.0", port=8000, reload=True)
