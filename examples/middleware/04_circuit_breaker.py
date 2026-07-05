"""Middleware: Circuit breaker — fail-fast on cascading failures.

CircuitBreakerMiddleware wraps consume and publish operations.
When the circuit is OPEN, messages are nacked immediately without
calling the handler (fail-fast pattern).

Run:
    python examples/middleware/04_circuit_breaker.py

Requirements:
    pip install "rabbitkit[async,obskit]"   # obskit provides CircuitBreaker
    RabbitMQ running on localhost:5672

Note:
    This example shows the interface. For a full circuit breaker, use obskit.
    If obskit is not available, a simple demo CB is used instead.
"""

import asyncio

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.middleware.circuit_breaker import CircuitBreakerMiddleware


# ── Simple demo circuit breaker (if obskit is not installed) ─────────────────
class SimpleCircuitBreaker:
    """Minimal circuit breaker for demonstration purposes."""

    def __init__(self, name: str, fail_max: int = 3, reset_timeout: float = 10.0) -> None:
        self.name = name
        self._failures = 0
        self._fail_max = fail_max
        self._reset_timeout = reset_timeout
        self._open = False
        self._opened_at: float = 0.0

    @property
    def state(self) -> str:
        return "open" if self._open else "closed"

    def call(self, func: "object", *args: "object", **kwargs: "object") -> "object":
        import time
        if self._open:
            if time.monotonic() - self._opened_at > self._reset_timeout:
                print(f"[CB:{self.name}] resetting to half-open")
                self._open = False
                self._failures = 0
            else:
                from rabbitkit.middleware.circuit_breaker import CircuitBreakerOpenError
                raise CircuitBreakerOpenError(f"Circuit {self.name} is OPEN")
        try:
            result = func(*args, **kwargs)  # type: ignore[operator]
            self._failures = 0
            return result
        except Exception as exc:
            self._failures += 1
            if self._failures >= self._fail_max:
                self._open = True
                import time
                self._opened_at = time.monotonic()
                print(f"[CB:{self.name}] OPENED after {self._failures} failures")
            raise


cb_consume = SimpleCircuitBreaker("orders-consume", fail_max=3, reset_timeout=10.0)
cb_publish = SimpleCircuitBreaker("orders-publish", fail_max=5, reset_timeout=5.0)

cb_mw = CircuitBreakerMiddleware(
    circuit_breaker=cb_consume,
    publish_circuit_breaker=cb_publish,
)

broker = AsyncBroker(RabbitConfig())

failure_count = 0

@broker.subscriber(queue="cb-demo", middlewares=[cb_mw])
async def handle_order(body: bytes) -> None:
    global failure_count
    failure_count += 1
    print(f"[handler] processing (failure #{failure_count}): {body.decode()}")
    if failure_count <= 3:
        raise ConnectionError(f"downstream service down (attempt {failure_count})")
    print("[handler] SUCCESS — downstream recovered")


async def main() -> None:
    await broker.start()

    print("Sending 6 messages — first 3 fail, circuit opens, last 3 are fast-rejected...")
    for i in range(6):
        await broker.publish(MessageEnvelope(
            routing_key="cb-demo",
            body=f"message-{i}".encode(),
        ))
        await asyncio.sleep(0.2)

    print(f"\nCircuit state: {cb_consume.state}")
    print("Waiting 10s for circuit to reset...")
    await asyncio.sleep(10)

    print("Sending 1 more message after reset...")
    await broker.publish(MessageEnvelope(routing_key="cb-demo", body=b"after-reset"))
    await asyncio.sleep(0.5)

    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
