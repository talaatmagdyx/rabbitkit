"""Middleware: Retry with delay queues and DLQ.

rabbitkit retry uses TTL + Dead-Letter-Exchange topology:
  queue → queue.retry.0 (5s TTL) → queue → queue.retry.1 (30s) → ... → queue.dlq

Two things are required for retry to actually happen:
  1. retry=RetryConfig(...) on the subscriber (or broker default) — declares the
     delay-queue + DLQ topology.
  2. RetryMiddleware in middlewares=[...], wired with publish_async_fn=broker.publish
     — this is what classifies failures and routes transient ones to the delay
     queue. Without it, nothing backs off (retry=RetryConfig alone only declares
     topology). Use AckPolicy.NACK_ON_ERROR so terminal failures dead-letter to
     the DLQ instead of requeue-looping.

Run:
    python examples/middleware/01_retry.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import MessageEnvelope, RabbitConfig, RetryConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.config import RetryDisabled
from rabbitkit.core.types import AckPolicy
from rabbitkit.middleware.retry import RetryMiddleware

# ── Broker-level retry default (declares topology for inheriting routes) ───────
config = RabbitConfig(
    retry=RetryConfig(
        max_retries=3,
        delays=(5, 30, 120),  # seconds: 5s then 30s then 120s
        jitter_factor=0.1,  # ±10% jitter to prevent thundering herd
    )
)
broker = AsyncBroker(config)

# RetryMiddleware must be wired with the broker's publish fn so it can route
# failures to the delay queues (and so a failed retry-publish nacks, not acks).
default_retry_mw = RetryMiddleware(config.retry, publish_async_fn=broker.publish)


# ── Default retry (inherits broker topology + uses RetryMiddleware) ────────────
attempt_count: dict[str, int] = {}


@broker.subscriber(
    queue="retryable-tasks",
    ack_policy=AckPolicy.NACK_ON_ERROR,
    middlewares=[default_retry_mw],
)
async def handle_retryable(body: bytes) -> None:
    """Fails the first 2 attempts, succeeds on the 3rd."""
    task_id = body.decode()
    attempt_count[task_id] = attempt_count.get(task_id, 0) + 1
    attempt = attempt_count[task_id]

    print(f"[retry demo] attempt #{attempt} for task={task_id!r}")

    if attempt < 3:
        # ConnectionError ⊂ OSError → classified TRANSIENT → routed to delay queue.
        raise ConnectionError(f"service unavailable on attempt {attempt}")

    print(f"[retry demo] SUCCESS on attempt #{attempt}!")


# ── Per-route retry override ──────────────────────────────────────────────────
critical_retry = RetryConfig(max_retries=5, delays=(2, 5, 10, 30, 60), jitter_factor=0.05)
critical_retry_mw = RetryMiddleware(critical_retry, publish_async_fn=broker.publish)


@broker.subscriber(
    queue="critical-tasks",
    retry=critical_retry,
    ack_policy=AckPolicy.NACK_ON_ERROR,
    middlewares=[critical_retry_mw],
)
async def handle_critical(body: bytes) -> None:
    """Critical tasks get more retry attempts; exhaustion → DLQ."""
    print(f"[critical] processing: {body.decode()}")
    raise ConnectionError("simulated failure")  # will exhaust retries → DLQ


# ── Disable retry for specific routes ────────────────────────────────────────
@broker.subscriber(queue="fire-forget", retry=RetryDisabled(), ack_policy=AckPolicy.NACK_ON_ERROR)
async def handle_fire_forget(body: bytes) -> None:
    """No retry — permanent failure is nacked straight to the DLQ."""
    print(f"[fire-forget] {body.decode()}")
    raise ValueError("permanent failure — no retry")


# ── Inspect DLQ after exhaustion ─────────────────────────────────────────────
# After retries are exhausted, messages land in "<queue>.dlq" (e.g.
# "retryable-tasks.dlq"). Inspect/replay with DLQInspector:
#
#   from rabbitkit.dlq import DLQInspector
#   inspector = DLQInspector(broker._transport)
#   msgs = await inspector.peek_async("retryable-tasks.dlq", limit=10)
#   count = await inspector.replay_async("retryable-tasks.dlq", target_queue="retryable-tasks")


async def main() -> None:
    await broker.start()

    await broker.publish(MessageEnvelope(routing_key="retryable-tasks", body=b"task-abc"))

    print("Waiting for retries (first retry after ~5s)...")
    await asyncio.sleep(15)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
