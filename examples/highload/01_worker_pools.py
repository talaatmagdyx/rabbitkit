"""High-load: Worker pools — concurrent message processing.

SyncWorkerPool uses a ThreadPoolExecutor.
AsyncWorkerPool uses asyncio.Semaphore to limit concurrent coroutines.

Run:
    python examples/highload/01_worker_pools.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import time

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.config import WorkerConfig

# ── Async broker with worker concurrency ─────────────────────────────────────
broker = AsyncBroker(RabbitConfig())


@broker.subscriber(queue="parallel-tasks")
async def handle_task(body: bytes) -> None:
    """Simulate a slow I/O-bound task (e.g. HTTP call, DB query)."""
    import json
    data = json.loads(body)
    task_id = data["id"]
    duration = data.get("duration", 0.5)

    print(f"[worker] task-{task_id} started (will take {duration}s)")
    await asyncio.sleep(duration)
    print(f"[worker] task-{task_id} done")


async def main_async_pool() -> None:
    """Async broker with AsyncWorkerPool for concurrent coroutines."""
    worker_config = WorkerConfig(
        worker_count=4,          # max 4 concurrent handlers
        prefetch_per_worker=2,   # prefetch = 4x2 = 8 messages ahead
    )
    await broker.start(worker_config=worker_config)
    print(f"AsyncBroker started with {worker_config.worker_count} concurrent workers")

    import json
    start = time.monotonic()

    # Publish 8 tasks of 0.5s each
    # With 4 workers → should complete in ~1s (2 batches of 4)
    # Without workers → would take 4s (sequential)
    for i in range(8):
        await broker.publish(MessageEnvelope(
            routing_key="parallel-tasks",
            body=json.dumps({"id": i, "duration": 0.5}).encode(),
        ))

    await asyncio.sleep(2.5)   # wait for all tasks to finish
    elapsed = time.monotonic() - start
    print(f"\nCompleted 8x0.5s tasks in {elapsed:.1f}s with {worker_config.worker_count} workers")
    print(f"(Sequential would take {8 * 0.5:.1f}s)")

    await broker.stop()


# ── Sync broker with SyncWorkerPool ──────────────────────────────────────────

def demo_sync_pool() -> None:
    """Sync broker with SyncWorkerPool (ThreadPoolExecutor)."""
    from rabbitkit.sync import SyncBroker

    sync_broker = SyncBroker(RabbitConfig())

    @sync_broker.subscriber(queue="sync-parallel-tasks")
    def handle_sync_task(body: bytes) -> None:
        import json
        import threading
        data = json.loads(body)
        print(f"[sync-worker] thread={threading.current_thread().name} task-{data['id']}")
        time.sleep(data.get("duration", 0.2))
        print(f"[sync-worker] task-{data['id']} done")

    worker_config = WorkerConfig(worker_count=4, prefetch_per_worker=2)
    sync_broker.start(worker_config=worker_config)

    import json
    for i in range(8):
        sync_broker.publish(MessageEnvelope(
            routing_key="sync-parallel-tasks",
            body=json.dumps({"id": i, "duration": 0.2}).encode(),
        ))

    time.sleep(1.5)  # wait for workers
    sync_broker.stop()
    print("SyncWorkerPool demo complete.")


if __name__ == "__main__":
    print("=== AsyncWorkerPool demo ===")
    asyncio.run(main_async_pool())

    print("\n=== SyncWorkerPool demo ===")
    demo_sync_pool()
