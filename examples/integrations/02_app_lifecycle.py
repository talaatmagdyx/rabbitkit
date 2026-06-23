"""Integration: RabbitApp — startup/shutdown hooks and lifecycle management.

RabbitApp manages ordered startup/shutdown with:
  - @app.on_startup / @app.after_startup
  - @app.on_shutdown / @app.after_shutdown
  - Signal handling (SIGINT / SIGTERM)
  - AppState tracking: IDLE → STARTING → RUNNING → STOPPING → STOPPED

Run:
    python examples/integrations/02_app_lifecycle.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import RabbitConfig, MessageEnvelope, RabbitApp, AppState
from rabbitkit.async_ import AsyncBroker

# ── App and broker ────────────────────────────────────────────────────────────
app = RabbitApp(title="order-service", version="2.1.0")
broker = AsyncBroker(RabbitConfig())


# ── Startup hooks ─────────────────────────────────────────────────────────────

@app.on_startup
async def connect_database() -> None:
    """Called first during startup — before broker starts consuming."""
    print("[lifecycle] Connecting to database...")
    await asyncio.sleep(0.1)  # simulate DB connect
    print("[lifecycle] Database connected")


@app.on_startup
async def warm_cache() -> None:
    """Called second during startup."""
    print("[lifecycle] Warming cache...")
    await asyncio.sleep(0.05)
    print("[lifecycle] Cache warmed")


@app.after_startup
async def log_ready() -> None:
    """Called after all on_startup hooks and broker is running."""
    print(f"[lifecycle] Service is READY — state={app.state.name}")
    print(f"[lifecycle] Broker routes: {len(broker.routes)}")


# ── Shutdown hooks ────────────────────────────────────────────────────────────

@app.on_shutdown
async def drain_queue() -> None:
    """Called during shutdown — before broker stops."""
    print("[lifecycle] Draining in-progress tasks...")
    await asyncio.sleep(0.1)
    print("[lifecycle] Drained")


@app.on_shutdown
async def disconnect_database() -> None:
    print("[lifecycle] Disconnecting database...")
    await asyncio.sleep(0.05)
    print("[lifecycle] Database disconnected")


@app.after_shutdown
async def log_stopped() -> None:
    print(f"[lifecycle] Service STOPPED — state={app.state.name}")


# ── Handler ───────────────────────────────────────────────────────────────────

@broker.subscriber(queue="lifecycle-demo")
async def handle(body: bytes) -> None:
    print(f"[handler] {body.decode()}")


# ── State tracking ────────────────────────────────────────────────────────────

async def check_state_during_lifecycle() -> None:
    """Monitors AppState transitions."""
    while app.state != AppState.RUNNING:
        await asyncio.sleep(0.01)
    print(f"\nApp is RUNNING — routes={len(broker.routes)}")

    # Publish a test message while running
    await broker.publish(MessageEnvelope(
        routing_key="lifecycle-demo",
        body=b"test message while running",
    ))
    await asyncio.sleep(0.2)

    # Initiate graceful shutdown
    print("\nInitiating graceful shutdown...")
    app.request_shutdown()


async def main() -> None:
    print("=== RabbitApp Lifecycle Demo ===")
    print(f"Initial state: {app.state.name}")  # IDLE

    # Run state monitor and lifecycle together
    asyncio.create_task(check_state_during_lifecycle())

    # run_async() starts app + broker, waits for stop signal
    await app.run_async(broker=broker)

    print(f"\nFinal state: {app.state.name}")  # STOPPED


# ── Alternative: manual lifecycle control ────────────────────────────────────
async def manual_lifecycle() -> None:
    """Explicit start/stop without run_async()."""
    await app.start_async()
    await broker.start()
    # ... run ...
    await broker.stop()
    await app.stop_async()


if __name__ == "__main__":
    asyncio.run(main())
