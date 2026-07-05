"""Middleware: Handler timeout — hard limit per message.

TimeoutMiddleware aborts handlers that exceed a time limit.
Async: asyncio.wait_for. Sync: threading.Thread + join(timeout).

Run:
    python examples/middleware/07_timeout.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import MessageEnvelope, RabbitConfig, RetryConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.middleware.timeout import HandlerTimeoutError, TimeoutConfig, TimeoutMiddleware

broker = AsyncBroker(RabbitConfig(
    retry=RetryConfig(max_retries=2, delays=(1, 3)),  # retry on timeout
))


# ── Basic timeout ─────────────────────────────────────────────────────────────
fast_timeout_mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=2.0))

@broker.subscriber(queue="timeout-demo", middlewares=[fast_timeout_mw])
async def handle_with_timeout(body: bytes) -> None:
    """Demonstrates a handler that sometimes exceeds the timeout."""
    import json
    data = json.loads(body)
    sleep_for = data.get("sleep", 0.1)

    print(f"[timeout] starting handler — will sleep {sleep_for}s (timeout=2s)")
    await asyncio.sleep(sleep_for)
    print(f"[timeout] completed in {sleep_for}s")


# ── Timeout combined with retry ───────────────────────────────────────────────
# HandlerTimeoutError is classified as TRANSIENT by default
# so retry middleware will re-queue the message.

slow_timeout_mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=5.0))

attempt_count: dict[str, int] = {}

@broker.subscriber(queue="timeout-retry", middlewares=[slow_timeout_mw])
async def handle_timeout_with_retry(body: bytes) -> None:
    """Simulates a service that's slow on first attempt but fast on retry."""
    import json
    data = json.loads(body)
    task_id = data["task_id"]
    attempt_count[task_id] = attempt_count.get(task_id, 0) + 1
    attempt = attempt_count[task_id]

    if attempt == 1:
        print(f"[timeout+retry] task={task_id} attempt #1 — sleeping 10s (will timeout)")
        await asyncio.sleep(10)  # will be cancelled by timeout
    else:
        print(f"[timeout+retry] task={task_id} attempt #{attempt} — fast path!")
        await asyncio.sleep(0.1)


# ── Per-queue timeout tuning ──────────────────────────────────────────────────
# Different queues can have very different timeout requirements

@broker.subscriber(
    queue="quick-health-checks",
    middlewares=[TimeoutMiddleware(TimeoutConfig(timeout_seconds=1.0))],
)
async def handle_quick(body: bytes) -> None:
    """Health checks must respond within 1 second."""
    await asyncio.sleep(0.05)
    print(f"[quick] health check passed: {body.decode()}")


@broker.subscriber(
    queue="ml-inference",
    middlewares=[TimeoutMiddleware(TimeoutConfig(timeout_seconds=30.0))],
)
async def handle_ml(body: bytes) -> None:
    """ML inference can take up to 30 seconds."""
    print(f"[ml] running inference on: {body.decode()[:50]}")
    await asyncio.sleep(0.5)  # simulated inference
    print("[ml] inference complete")


async def main() -> None:
    await broker.start()
    import json

    # Fast message — completes within timeout
    await broker.publish(MessageEnvelope(
        routing_key="timeout-demo",
        body=json.dumps({"sleep": 0.5}).encode(),
    ))

    # Slow message — exceeds 2s timeout → HandlerTimeoutError → retry/DLQ
    await broker.publish(MessageEnvelope(
        routing_key="timeout-demo",
        body=json.dumps({"sleep": 5.0}).encode(),
    ))

    await asyncio.sleep(4)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
