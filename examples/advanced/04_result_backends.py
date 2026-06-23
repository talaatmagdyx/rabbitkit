"""Advanced: Result backends — fire-and-retrieve pattern.

Store handler return values in Redis so callers can retrieve them
by correlation_id. Enables async request/response without dedicated RPC queues.

Run:
    python examples/advanced/04_result_backends.py

Requirements:
    pip install "rabbitkit[async,redis]"
    RabbitMQ running on localhost:5672
    Redis running on localhost:6379
"""

import asyncio
import json
import uuid

from rabbitkit import RabbitConfig, MessageEnvelope
from rabbitkit.async_ import AsyncBroker
from rabbitkit.results.backend import RedisResultBackend
from rabbitkit.results.middleware import ResultMiddleware

try:
    import redis.asyncio as aioredis
    r = aioredis.Redis(host="localhost", port=6379)
except ImportError:
    print("redis not installed — run: pip install redis")
    raise

broker = AsyncBroker(RabbitConfig())

# ── Set up result backend and middleware ──────────────────────────────────────
backend = RedisResultBackend(r, key_prefix="myapp:result:")
result_mw = ResultMiddleware(backend, ttl=300)  # store for 5 minutes


# ── Handlers that return values ───────────────────────────────────────────────

@broker.subscriber(queue="compute", middlewares=[result_mw])
async def compute_fibonacci(body: bytes) -> bytes:
    """Compute Fibonacci number and store result in Redis."""
    data = json.loads(body)
    n = data["n"]

    def fib(x: int) -> int:
        a, b = 0, 1
        for _ in range(x):
            a, b = b, a + b
        return a

    result = fib(n)
    print(f"[compute] fib({n}) = {result}, storing with correlation_id={data.get('correlation_id', '?')[:8]}")
    return json.dumps({"n": n, "result": result}).encode()


@broker.subscriber(queue="summarize", middlewares=[result_mw])
async def summarize_text(body: bytes) -> bytes:
    """Simulate a summarization task."""
    data = json.loads(body)
    text = data["text"]
    # Simulate slow processing
    await asyncio.sleep(0.2)
    summary = text[:50] + "..." if len(text) > 50 else text
    return json.dumps({"original_length": len(text), "summary": summary}).encode()


# ── Client side: fire and retrieve ───────────────────────────────────────────

async def fire_and_retrieve(
    routing_key: str,
    request: dict,  # type: ignore[type-arg]
    poll_interval: float = 0.05,
    max_wait: float = 5.0,
) -> dict | None:  # type: ignore[type-arg]
    """Send a message and poll for its result."""
    corr_id = str(uuid.uuid4())

    await broker.publish(MessageEnvelope(
        routing_key=routing_key,
        body=json.dumps({**request, "correlation_id": corr_id}).encode(),
        correlation_id=corr_id,
    ))

    # Poll until result is available
    import time
    deadline = time.monotonic() + max_wait
    while time.monotonic() < deadline:
        result = await backend.fetch_async(corr_id)
        if result is not None:
            return json.loads(result)
        await asyncio.sleep(poll_interval)

    return None  # timeout


async def main() -> None:
    await broker.start()
    await asyncio.sleep(0.1)  # let consumers register

    print("=== Fire-and-retrieve: compute Fibonacci numbers ===")

    # Submit multiple tasks and wait for results
    for n in [10, 20, 30, 35]:
        result = await fire_and_retrieve("compute", {"n": n})
        if result:
            print(f"  fib({n}) = {result['result']}")
        else:
            print(f"  fib({n}) timed out!")

    print("\n=== Fire-and-retrieve: text summarization ===")
    long_text = "The quick brown fox jumps over the lazy dog. " * 10
    result = await fire_and_retrieve("summarize", {"text": long_text})
    if result:
        print(f"  Original: {result['original_length']} chars")
        print(f"  Summary:  {result['summary']!r}")

    print("\n=== Concurrent requests ===")
    tasks = [
        fire_and_retrieve("compute", {"n": n})
        for n in range(5, 50, 5)
    ]
    results = await asyncio.gather(*tasks)
    for n, result in zip(range(5, 50, 5), results):
        if result:
            print(f"  fib({n:2d}) = {result['result']}")

    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
