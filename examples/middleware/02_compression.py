"""Middleware: gzip and zstd compression.

CompressionMiddleware compresses outgoing publishes and decompresses
incoming messages automatically using content-encoding headers.

Run:
    python examples/middleware/02_compression.py

Requirements:
    pip install "rabbitkit[async]"          # gzip (built-in)
    pip install "rabbitkit[async,compression]"  # + zstd
    RabbitMQ running on localhost:5672
"""

import asyncio
import json

from rabbitkit import CompressionConfig, MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker
from rabbitkit.middleware.compression import CompressionMiddleware

# ── gzip compression (no extra deps) ─────────────────────────────────────────
gzip_mw = CompressionMiddleware(CompressionConfig(
    algorithm="gzip",
    threshold=512,     # only compress bodies >= 512 bytes
    level=6,           # compression level 1-9 (6 = balanced)
))

broker = AsyncBroker(RabbitConfig())


@broker.subscriber(queue="compressed-events", middlewares=[gzip_mw])
async def handle_compressed(body: bytes) -> None:
    """Body is automatically decompressed before reaching the handler."""
    data = json.loads(body)
    print(f"[gzip] decompressed {len(body)} bytes: keys={list(data.keys())}")


# ── zstd compression (requires zstandard package) ────────────────────────────
try:
    zstd_mw = CompressionMiddleware(CompressionConfig(
        algorithm="zstd",
        threshold=256,
        level=3,    # zstd level 1-22 (3 = fast, good ratio)
    ))

    @broker.subscriber(queue="zstd-events", middlewares=[zstd_mw])
    async def handle_zstd(body: bytes) -> None:
        data = json.loads(body)
        print(f"[zstd] decompressed {len(body)} bytes")
except Exception:
    print("zstd not available — skipping (pip install zstandard)")


# ── Auto-pass-through for uncompressed messages ───────────────────────────────
# If a message doesn't have content-encoding: gzip (or zstd),
# CompressionMiddleware passes it through untouched.


# ── Compression on publish side ───────────────────────────────────────────────
# The same middleware compresses outgoing MessageEnvelope bodies.
# Use it in publish_scope by attaching to the broker (or use it manually):
#
#   compressed_body = gzip_mw._compressor.compress(raw_body)


async def main() -> None:
    await broker.start()

    # Build a large payload that exceeds the threshold
    large_payload = json.dumps({
        "event": "user.updated",
        "user_id": 42,
        "data": {"field_" + str(i): "value_" + str(i) for i in range(50)},
    }).encode()

    print(f"Original payload size: {len(large_payload)} bytes")

    # The gzip_mw compresses this on publish (via publish_scope)
    await broker.publish(MessageEnvelope(
        routing_key="compressed-events",
        body=large_payload,
        # content_encoding is set automatically by CompressionMiddleware
    ))

    # Small payload — below threshold, sent uncompressed
    small_payload = b'{"event": "ping"}'
    await broker.publish(MessageEnvelope(
        routing_key="compressed-events",
        body=small_payload,
    ))

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
