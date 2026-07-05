"""kubernetes_worker — production-ready consumer for Kubernetes deployment.

Demonstrates best practices for running rabbitkit in a pod:
- Config from environment variables (RABBITMQ_HOST, RABBITMQ_PORT, etc.)
- Liveness and readiness health checks via HTTP
- Graceful shutdown on SIGTERM (drains in-flight messages before exiting)
- Structured JSON logging for log aggregators (Loki, Elasticsearch)
- Retry with DLQ for transient failures

See the companion kubernetes.yaml for pod manifest, probes, and preStop hook.
See docs/kubernetes.md for the full deployment guide.

Run locally:
    pip install rabbitkit[all]
    export RABBITMQ_HOST=localhost
    docker run -d -p 5672:5672 rabbitmq:3.13-management-alpine
    python worker.py

In Kubernetes:
    kubectl apply -f kubernetes.yaml
"""

from __future__ import annotations

import asyncio
import os
import signal

from rabbitkit import AsyncBroker
from rabbitkit.core.config import (
    ConnectionConfig,
    ConsumerConfig,
    LoggingConfig,
    RabbitConfig,
    RetryConfig,
    WorkerConfig,
)
from rabbitkit.health import broker_health_check_async


def make_config() -> RabbitConfig:
    return RabbitConfig(
        connection=ConnectionConfig(
            host=os.environ.get("RABBITMQ_HOST", "localhost"),
            port=int(os.environ.get("RABBITMQ_PORT", "5672")),
            username=os.environ.get("RABBITMQ_USER", "guest"),
            password=os.environ.get("RABBITMQ_PASSWORD", "guest"),
            vhost=os.environ.get("RABBITMQ_VHOST", "/"),
        ),
        retry=RetryConfig(max_retries=3, delays=(5, 30, 120)),
        consumer=ConsumerConfig(
            prefetch_count=int(os.environ.get("PREFETCH_COUNT", "20")),
            graceful_timeout=30.0,  # drain up to 30s on SIGTERM
        ),
        logging=LoggingConfig(
            render_json=True,  # structured JSON for Loki / Elasticsearch
        ),
    )


config = make_config()
broker = AsyncBroker(config)


@broker.subscriber(queue=os.environ.get("QUEUE_NAME", "k8s-orders"))
async def handle_order(body: dict) -> None:
    order_id = body.get("id")
    print(f"processing order {order_id}")
    # ... your business logic here ...
    print(f"order {order_id} done")


async def health_server() -> None:
    """Minimal HTTP health server for Kubernetes probes on port 8080."""
    from aiohttp import web  # type: ignore[import-untyped]

    async def liveness(request: web.Request) -> web.Response:
        check = await broker_health_check_async(broker)
        status = 200 if check["status"] == "healthy" else 503
        return web.json_response(check, status=status)

    async def readiness(request: web.Request) -> web.Response:
        check = await broker_health_check_async(broker)
        ready = check["status"] == "healthy" and check.get("consumers_active", False)
        status = 200 if ready else 503
        return web.json_response(check, status=status)

    app = web.Application()
    app.router.add_get("/healthz/live", liveness)
    app.router.add_get("/healthz/ready", readiness)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    print("health server listening on :8080")


async def main() -> None:
    await broker.start(worker_config=WorkerConfig(worker_count=1))
    print(f"consuming from queue={os.environ.get('QUEUE_NAME', 'orders')}")

    try:
        await health_server()
    except ImportError:
        print("aiohttp not installed — health server skipped (pip install aiohttp)")

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    print("running. SIGTERM triggers graceful drain.")
    await stop.wait()

    print("shutting down gracefully...")
    await broker.stop()
    print("stopped.")


if __name__ == "__main__":
    asyncio.run(main())
