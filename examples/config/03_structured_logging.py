"""Configuration: Structured logging with structlog.

LoggingConfig activates structlog so rabbitkit emits structured log events.
Per-message context (message_id, routing_key, queue, handler) is bound
automatically and cleared after each message.

Run:
    python examples/config/03_structured_logging.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import RabbitConfig, MessageEnvelope
from rabbitkit.async_ import AsyncBroker
from rabbitkit.core.logging import LoggingConfig, configure_structlog


# ─────────────────────────────────────────────────────────────────────────────
# Mode 1: Development — coloured console output
# ─────────────────────────────────────────────────────────────────────────────

dev_logging = LoggingConfig(
    render_json=False,           # human-readable console (coloured with structlog.dev)
    add_log_level=True,          # include level (info/warning/error)
    timestamper_fmt="iso",       # ISO 8601 timestamps
    include_caller_info=False,   # skip filename:lineno in dev
)

dev_broker = AsyncBroker(
    RabbitConfig(logging=dev_logging)
)


# ─────────────────────────────────────────────────────────────────────────────
# Mode 2: Production — JSON lines for log aggregators
# ─────────────────────────────────────────────────────────────────────────────
#
# prod_logging = LoggingConfig(
#     render_json=True,            # {"event": "...", "level": "info", "timestamp": "..."}
#     add_log_level=True,
#     timestamper_fmt="iso",
#     include_caller_info=False,
# )
# prod_broker = AsyncBroker(RabbitConfig(logging=prod_logging))


# ─────────────────────────────────────────────────────────────────────────────
# Mode 3: Manual configuration (without RabbitConfig.logging)
# ─────────────────────────────────────────────────────────────────────────────
# configure_structlog(LoggingConfig(render_json=True))


# ── Register handlers ─────────────────────────────────────────────────────────
import structlog

logger = structlog.stdlib.get_logger(__name__)


@dev_broker.subscriber(queue="log-demo")
async def handle_event(body: bytes) -> None:
    """All log calls here automatically include message_id, routing_key, queue, handler."""
    import json
    try:
        data = json.loads(body)
        logger.info("processing event", event_type=data.get("type"), user_id=data.get("user_id"))
        await asyncio.sleep(0.01)
        logger.info("event processed successfully")
    except Exception as exc:
        logger.error("failed to process event", error=str(exc))
        raise


@dev_broker.subscriber(queue="log-errors")
async def handle_errors(body: bytes) -> None:
    """Demonstrates error logging with context."""
    logger.warning("received potentially problematic message", body_length=len(body))
    if body == b"fail":
        logger.error("rejecting bad message")
        raise ValueError("bad message content")
    logger.info("processed", size=len(body))


# ── Bind extra context ────────────────────────────────────────────────────────
# You can add custom context to every log line within a scope:
#
#   import structlog
#   structlog.contextvars.bind_contextvars(
#       tenant="acme",
#       request_id="req-abc-123",
#   )
#   logger.info("processing")  # includes tenant=acme, request_id=req-abc-123
#   structlog.contextvars.clear_contextvars()


async def main() -> None:
    await dev_broker.start()
    print("Broker started with structured logging. Check the console output below:\n")

    import json
    await dev_broker.publish(MessageEnvelope(
        routing_key="log-demo",
        body=json.dumps({"type": "user.login", "user_id": 42}).encode(),
        message_id="msg-001",
    ))

    await dev_broker.publish(MessageEnvelope(
        routing_key="log-errors",
        body=b"healthy message",
        message_id="msg-002",
    ))

    await dev_broker.publish(MessageEnvelope(
        routing_key="log-errors",
        body=b"fail",
        message_id="msg-003",
    ))

    await asyncio.sleep(0.5)
    await dev_broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
