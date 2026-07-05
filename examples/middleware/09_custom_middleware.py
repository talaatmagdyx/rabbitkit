"""Middleware: Writing your own custom middleware.

BaseMiddleware provides no-op defaults for all hooks.
Override only the hooks you need.

Available hooks:
  on_receive(msg)           — notification on message arrival
  consume_scope(next, msg)  — wrap handler execution (sync)
  consume_scope_async(...)  — wrap handler execution (async)
  after_processed(msg, exc) — post-processing notification
  publish_scope(next, env)  — wrap outgoing publish (sync)
  publish_scope_async(...)  — wrap outgoing publish (async)

Run:
    python examples/middleware/09_custom_middleware.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio
import time
from typing import Any

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope as Envelope
from rabbitkit.middleware.base import BaseMiddleware

broker = AsyncBroker(RabbitConfig())


# ── Example 1: Logging middleware ─────────────────────────────────────────────

class LoggingMiddleware(BaseMiddleware):
    """Logs timing and outcome for every message."""

    def on_receive(self, message: RabbitMessage) -> None:
        print(f"[log] received: queue={message.routing_key!r} id={message.message_id!r}")

    async def consume_scope_async(self, call_next: Any, message: RabbitMessage) -> Any:
        start = time.monotonic()
        exc_info = None
        try:
            result = await call_next(message)
            return result
        except Exception as exc:
            exc_info = exc
            raise
        finally:
            elapsed = (time.monotonic() - start) * 1000
            status = "ERROR" if exc_info else "OK"
            print(f"[log] {status} in {elapsed:.1f}ms: {message.routing_key!r}")

    def consume_scope(self, call_next: Any, message: RabbitMessage) -> Any:
        start = time.monotonic()
        try:
            result = call_next(message)
            print(f"[log] OK in {(time.monotonic()-start)*1000:.1f}ms")
            return result
        except Exception as exc:
            print(f"[log] ERROR: {exc}")
            raise


# ── Example 2: Header injection middleware ────────────────────────────────────

class HeaderInjectionMiddleware(BaseMiddleware):
    """Adds standard headers to every outgoing publish."""

    def __init__(self, service_name: str, environment: str) -> None:
        self._service = service_name
        self._env = environment

    def publish_scope(self, call_next: Any, envelope: Envelope) -> Any:
        new_headers = dict(envelope.headers or {})
        new_headers.update({
            "x-source-service": self._service,
            "x-environment": self._env,
        })
        from dataclasses import replace
        enriched = replace(envelope, headers=new_headers)
        return call_next(enriched)

    async def publish_scope_async(self, call_next: Any, envelope: Envelope) -> Any:
        new_headers = dict(envelope.headers or {})
        new_headers.update({
            "x-source-service": self._service,
            "x-environment": self._env,
        })
        from dataclasses import replace
        enriched = replace(envelope, headers=new_headers)
        return await call_next(enriched)


# ── Example 3: Metrics middleware ─────────────────────────────────────────────

class SimpleMetricsMiddleware(BaseMiddleware):
    """Tracks message counts and error rates."""

    def __init__(self) -> None:
        self._processed = 0
        self._errors = 0
        self._total_ms = 0.0

    @property
    def stats(self) -> dict[str, Any]:
        return {
            "processed": self._processed,
            "errors": self._errors,
            "avg_ms": self._total_ms / max(self._processed, 1),
        }

    async def consume_scope_async(self, call_next: Any, message: RabbitMessage) -> Any:
        start = time.monotonic()
        try:
            result = await call_next(message)
            self._processed += 1
            return result
        except Exception:
            self._errors += 1
            raise
        finally:
            self._total_ms += (time.monotonic() - start) * 1000

    def after_processed(self, message: RabbitMessage, exc: Exception | None) -> None:
        if exc:
            print(f"[metrics] error: {type(exc).__name__}: {exc}")


# ── Wire up middlewares ───────────────────────────────────────────────────────

metrics_mw = SimpleMetricsMiddleware()
log_mw = LoggingMiddleware()
header_mw = HeaderInjectionMiddleware(service_name="order-service", environment="dev")


@broker.subscriber(
    queue="custom-mw-demo",
    middlewares=[log_mw, metrics_mw],   # applied in order: log wraps metrics wraps handler
)
async def handle_event(body: bytes) -> None:
    print(f"[handler] {body.decode()}")
    await asyncio.sleep(0.01)


async def main() -> None:
    await broker.start()

    for i in range(5):
        await broker.publish(MessageEnvelope(
            routing_key="custom-mw-demo",
            body=f"event-{i}".encode(),
        ))

    await asyncio.sleep(0.5)
    print(f"\nMetrics: {metrics_mw.stats}")
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
