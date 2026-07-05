"""Middleware: OpenTelemetry tracing via OTelTracingMiddleware.

Wraps consume and publish operations in OTel trace spans with RabbitMQ
semantic attributes (messaging.system, messaging.operation, etc.)

Run:
    python examples/middleware/08_tracing.py

Requirements:
    pip install "rabbitkit[async,obskit]"
    RabbitMQ running on localhost:5672

Note:
    Without obskit, OTelTracingMiddleware is a no-op passthrough.
    A loud construction-time warning tells you tracing is a no-op.
"""

import asyncio

from rabbitkit import MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.middleware.otel import OTelTracingMiddleware

broker = AsyncBroker(RabbitConfig())

# ── Basic tracing setup ───────────────────────────────────────────────────────
# OTelTracingMiddleware is a no-op if obskit is not installed.
# When obskit IS installed, it creates spans with:
#   messaging.system          = "rabbitmq"
#   messaging.operation       = "receive" | "publish"
#   messaging.destination     = queue name
#   messaging.rabbitmq.routing_key = routing key
#   messaging.message_id      = message_id header

tracing_mw = OTelTracingMiddleware(service_name="order-service")


@broker.subscriber(queue="traced-orders", middlewares=[tracing_mw])
async def handle_order(body: bytes) -> None:
    """Each message creates a trace span wrapping this handler."""
    print(f"[traced] processing order: {body.decode()}")
    await asyncio.sleep(0.01)  # simulated work
    print("[traced] done")


# ── Recommended middleware ordering ──────────────────────────────────────────
# Tracing should be OUTERMOST so it wraps everything including retry logic.
#
# from rabbitkit import RetryConfig
# from rabbitkit.middleware.retry import RetryMiddleware
#
# retry_mw = RetryMiddleware(RetryConfig(max_retries=3, delays=(5, 30, 120)))
# tracing_mw = OTelTracingMiddleware(service_name="order-service")
#
# @broker.subscriber(
#     queue="orders",
#     middlewares=[tracing_mw, retry_mw],  # tracing outermost
# )
# async def handle(body: bytes) -> None: ...


# ── Configure OTel exporter (OTLP/Jaeger/Zipkin) ─────────────────────────────
# Set up your exporter before starting the broker:
#
# from opentelemetry import trace
# from opentelemetry.sdk.trace import TracerProvider
# from opentelemetry.sdk.trace.export import BatchSpanProcessor
# from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
#
# provider = TracerProvider()
# provider.add_span_processor(
#     BatchSpanProcessor(OTLPSpanExporter(endpoint="http://jaeger:4317"))
# )
# trace.set_tracer_provider(provider)
#
# Then use OTelTracingMiddleware as above — it will use the configured provider.


async def main() -> None:
    await broker.start()

    import uuid
    for i in range(3):
        await broker.publish(MessageEnvelope(
            routing_key="traced-orders",
            body=f'{{"order_id": {i}}}'.encode(),
            message_id=str(uuid.uuid4()),
        ))

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
