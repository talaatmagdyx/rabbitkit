"""Quickstart: SyncBroker — blocking consumer loop.

Demonstrates the minimal setup needed to receive and publish messages
using the synchronous (pika-based) broker.

Run:
    python examples/quickstart/01_sync_broker.py

Requirements:
    pip install "rabbitkit[sync]"
    RabbitMQ running on localhost:5672
"""

from rabbitkit import RabbitConfig
from rabbitkit import MessageEnvelope
from rabbitkit.sync import SyncBroker

# ── 1. Create broker with default config (localhost:5672, guest/guest) ────────
broker = SyncBroker(RabbitConfig())


# ── 2. Register a handler with the @subscriber decorator ─────────────────────
@broker.subscriber(queue="greetings")
def handle_greeting(body: bytes) -> None:
    """Receive a greeting message and print it."""
    print(f"[handler] received: {body.decode()}")


# ── 3. Publish a test message then start consuming ────────────────────────────
def main() -> None:
    # start() connects, declares topology, begins consuming
    broker.start()

    print("Connected. Publishing a test message...")
    broker.publish(
        MessageEnvelope(
            routing_key="greetings",
            body=b"Hello from rabbitkit!",
        )
    )

    print("Waiting for messages (press Ctrl+C to stop)...")
    try:
        # start_consuming() blocks the thread until Ctrl+C
        broker._transport.start_consuming()  # type: ignore[union-attr]
    except KeyboardInterrupt:
        pass
    finally:
        broker.stop()
        print("Broker stopped.")


# Alternatively, use the convenience run() method which does start + block + stop:
#
#   broker.run()   # blocks until Ctrl+C


if __name__ == "__main__":
    main()
