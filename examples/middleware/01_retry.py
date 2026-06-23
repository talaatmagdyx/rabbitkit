"""Middleware: Retry with delay queues and DLQ.

rabbitkit retry uses TTL + Dead-Letter-Exchange topology:
  queue → queue.retry.0 (5s TTL) → queue → queue.retry.1 (30s) → ... → queue.dlq

Run:
    python examples/middleware/01_retry.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import RabbitConfig, MessageEnvelope, RetryConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.config import RetryDisabled

# ── Broker-level retry default ────────────────────────────────────────────────
# All subscribers inherit this retry config unless overridden.
config = RabbitConfig(
    retry=RetryConfig(
        max_retries=3,
        delays=(5, 30, 120),      # seconds: 5s then 30s then 120s
        jitter_factor=0.1,         # ±10% jitter to prevent thundering herd
    )
)
broker = AsyncBroker(config)


# ── Default retry (inherits broker config) ────────────────────────────────────
attempt_count: dict[str, int] = {}

@broker.subscriber(queue="retryable-tasks")
async def handle_retryable(body: bytes) -> None:
    """Fails the first 2 attempts, succeeds on the 3rd."""
    task_id = body.decode()
    attempt_count[task_id] = attempt_count.get(task_id, 0) + 1
    attempt = attempt_count[task_id]

    print(f"[retry demo] attempt #{attempt} for task={task_id!r}")

    if attempt < 3:
        # Simulate a transient failure (ConnectionError is classified TRANSIENT)
        raise ConnectionError(f"service unavailable on attempt {attempt}")

    print(f"[retry demo] SUCCESS on attempt #{attempt}!")


# ── Per-route retry override ──────────────────────────────────────────────────

@broker.subscriber(
    queue="critical-tasks",
    retry=RetryConfig(
        max_retries=5,
        delays=(2, 5, 10, 30, 60),
        jitter_factor=0.05,
    ),
)
async def handle_critical(body: bytes) -> None:
    """Critical tasks get more retry attempts with shorter initial delay."""
    print(f"[critical] processing: {body.decode()}")
    raise ConnectionError("simulated failure")  # will exhaust retries → DLQ


# ── Disable retry for specific routes ────────────────────────────────────────

@broker.subscriber(queue="fire-forget", retry=RetryDisabled())
async def handle_fire_forget(body: bytes) -> None:
    """No retry — failure goes straight to DLQ (or is dropped)."""
    print(f"[fire-forget] {body.decode()}")
    raise ValueError("permanent failure — no retry")


# ── Inspect DLQ after exhaustion ─────────────────────────────────────────────
# After all retries are exhausted, messages land in:
#   queue.dlq  (e.g., "retryable-tasks.dlq")
#
# Use DLQInspector to peek and replay:
#
#   from rabbitkit.dlq import DLQInspector
#   inspector = DLQInspector(transport)
#   msgs = inspector.peek("retryable-tasks.dlq", limit=10)
#   count = inspector.replay("retryable-tasks.dlq", target_queue="retryable-tasks")


async def main() -> None:
    await broker.start()

    await broker.publish(MessageEnvelope(
        routing_key="retryable-tasks",
        body=b"task-abc",
    ))

    print("Waiting for retries (this will take ~5s for first retry)...")
    await asyncio.sleep(15)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
