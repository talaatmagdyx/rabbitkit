"""FastAPI app with rabbitkit lifespan integration.

Uses rabbitkit_lifespan() to wire broker start/stop into FastAPI's lifespan.

Run:
    pip install "rabbitkit[async,fastapi]" uvicorn
    docker run -d --rm -p 5672:5672 rabbitmq:4-management
    python examples/fastapi_lifespan/app.py

Test:
    curl http://localhost:8000/health
    curl -X POST http://localhost:8000/orders \\
         -H "Content-Type: application/json" \\
         -d '{"order_id": "o-123", "item": "Widget"}'

Requirements:
    pip install "rabbitkit[async,fastapi]" uvicorn fastapi
"""

import json
from typing import Any

from rabbitkit import ConnectionConfig, MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.fastapi import rabbitkit_lifespan
from rabbitkit.health import broker_health_check_async

try:
    from fastapi import FastAPI, HTTPException
    from fastapi.responses import JSONResponse
except ImportError as exc:
    raise SystemExit("Install fastapi: pip install 'rabbitkit[fastapi]'") from exc

broker = AsyncBroker(RabbitConfig(connection=ConnectionConfig(host="localhost")))

app = FastAPI(
    title="Order Service",
    lifespan=lambda app: rabbitkit_lifespan(app, broker=broker),
)


@broker.subscriber(queue="orders", routing_key="orders.created")
async def handle_order(body: bytes) -> None:
    data = json.loads(body)
    print(f"[consumer] order received: {data.get('order_id')}")


@app.get("/health")
async def health() -> dict[str, Any]:
    result = await broker_health_check_async(broker)
    if not result.connected:
        raise HTTPException(status_code=503, detail="broker unavailable")
    return {"status": result.status.value, "connected": result.connected, "routes": result.route_count}


@app.post("/orders", status_code=202)
async def create_order(order: dict[str, Any]) -> dict[str, str]:
    if "order_id" not in order:
        raise HTTPException(status_code=400, detail="order_id required")
    await broker.publish(
        MessageEnvelope(
            routing_key="orders.created",
            body=json.dumps(order).encode(),
            content_type="application/json",
        )
    )
    return {"status": "accepted", "order_id": str(order["order_id"])}


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
