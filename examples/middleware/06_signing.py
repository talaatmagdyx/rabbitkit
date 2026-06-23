"""Middleware: HMAC message signing and verification.

Signs outgoing messages with HMAC-SHA256 and verifies incoming signatures.
No extra dependencies — uses stdlib hmac + hashlib.

Run:
    python examples/middleware/06_signing.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ running on localhost:5672
"""

import asyncio

from rabbitkit import RabbitConfig, MessageEnvelope
from rabbitkit.async_ import AsyncBroker
from rabbitkit.middleware.signing import (
    SigningMiddleware,
    SigningConfig,
    InvalidSignatureError,
)

broker = AsyncBroker(RabbitConfig())

SHARED_SECRET = "super-secret-key-do-not-commit"

# ── Both publisher and consumer share the same middleware ─────────────────────
# In practice, they'd be in different services with the same secret.
signing_mw = SigningMiddleware(
    SigningConfig(
        secret_key=SHARED_SECRET,
        algorithm="hmac-sha256",
        header_name="x-rabbitkit-signature",
        reject_unsigned=True,    # reject messages with no signature header
        reject_invalid=True,     # reject messages with wrong signature
    )
)


@broker.subscriber(queue="signed-events", middlewares=[signing_mw])
async def handle_signed(body: bytes) -> None:
    """Only processes messages with a valid HMAC signature."""
    print(f"[signed] verified message: {body.decode()}")


# ── Monitoring mode — log but don't reject ────────────────────────────────────
monitor_mw = SigningMiddleware(
    SigningConfig(
        secret_key=SHARED_SECRET,
        reject_unsigned=False,   # allow unsigned messages
        reject_invalid=False,    # allow invalid signatures (just log)
    )
)

@broker.subscriber(queue="monitored-events", middlewares=[monitor_mw])
async def handle_monitored(body: bytes) -> None:
    print(f"[monitored] {body.decode()}")


# ── SHA-512 for higher security ───────────────────────────────────────────────
strong_mw = SigningMiddleware(
    SigningConfig(
        secret_key=b"\x00very\xff long\xde random\xad key\xef" * 4,
        algorithm="hmac-sha512",
    )
)

@broker.subscriber(queue="strong-signed", middlewares=[strong_mw])
async def handle_strong(body: bytes) -> None:
    print(f"[sha512-signed] {body.decode()}")


async def main() -> None:
    await broker.start()

    # Publish a properly signed message (signing_mw signs on publish)
    print("Publishing signed message...")
    await broker.publish(MessageEnvelope(
        routing_key="signed-events",
        body=b'{"event": "payment.completed", "amount": 99.99}',
    ))

    # Publish a message WITHOUT a signature to trigger rejection
    print("Publishing unsigned message (should be rejected)...")
    await broker.publish(MessageEnvelope(
        routing_key="signed-events",
        body=b'{"event": "spoofed"}',
        # No x-rabbitkit-signature header → InvalidSignatureError → DLQ
    ))

    # Publish a message with a WRONG signature
    print("Publishing message with wrong signature (should be rejected)...")
    await broker.publish(MessageEnvelope(
        routing_key="signed-events",
        body=b'{"event": "tampered"}',
        headers={"x-rabbitkit-signature": "deadbeef" * 8},  # fake hex
    ))

    await asyncio.sleep(0.5)
    await broker.stop()


if __name__ == "__main__":
    asyncio.run(main())
