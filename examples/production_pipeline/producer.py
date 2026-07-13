"""production_pipeline/producer.py — production-grade CONFIRMED publishing (sync/pika).

Seeds the pipeline with traffic and demonstrates the publish-side contract:
``broker.publish()`` NEVER raises on delivery failure — it returns a
``PublishOutcome`` and you MUST branch on it. This producer checks every
outcome, shows what an unroutable (RETURNED) publish looks like, and uses
``outcome.classification`` to tell transient failures from permanent ones.

Topology is owned by app.py (the consumer) — run it first:
    python examples/production_pipeline/app.py      # terminal 1
    python examples/production_pipeline/producer.py # terminal 2

If the topology isn't there yet, this producer retries briefly with the
outcome telling it why, then gives up with a clear message (exit 0 — it's a
demo seeder, not a service; a real publisher would alert instead).

Sync publish-only note: nothing services pika's heartbeats between publishes.
This seeder publishes back-to-back and exits, so that's moot — a LONG-LIVED
publish-only SyncBroker must call ``broker.pump_idle()`` from its idle loop
(see docs/guide/full-guide.md, "sync vs async connection models").
"""

import json
import logging
import os
import sys
import time
import uuid

from rabbitkit import MessageEnvelope, PublishStatus, RabbitConfig
from rabbitkit.core.config import ConnectionConfig, PublisherConfig
from rabbitkit.sync import SyncBroker

logging.basicConfig(level=logging.WARNING)
log = logging.getLogger("production_pipeline.producer")

EXCHANGE = "pp.orders"
RK_CREATED = "order.created"

ORDERS = [
    {"order_id": "ord-1001", "customer_id": "cus-1", "amount_cents": 4_999, "currency": "USD"},
    {"order_id": "ord-1002", "customer_id": "cus-2", "amount_cents": 129_900, "currency": "EUR"},
    {"order_id": "ord-1003", "customer_id": "cus-3", "amount_cents": 799, "currency": "USD"},
    # Walks the retry ladder (2s, 10s, 30s), then dead-letters to
    # pp.orders.incoming.dlq with x-rabbitkit-error-* triage headers:
    {"order_id": "ord-2001", "customer_id": "cus-4", "amount_cents": 100, "currency": "USD", "simulate": "transient"},
    # PERMANENT error -> skips retries, dead-letters immediately:
    {"order_id": "ord-3001", "customer_id": "cus-5", "amount_cents": 100, "currency": "USD", "simulate": "permanent"},
]


def envelope(order: dict) -> MessageEnvelope:
    return MessageEnvelope(
        exchange=EXCHANGE,
        routing_key=RK_CREATED,
        body=json.dumps(order).encode(),
        # correlation_id ties producer logs to consumer logs to DLQ triage.
        correlation_id=f"corr-{order['order_id']}",
        message_id=f"msg-{order['order_id']}-{uuid.uuid4()}",  # dedup key
        # mandatory=True: an UNROUTABLE publish comes back as RETURNED
        # instead of being broker-confirmed into the void.
        mandatory=True,
    )


def publish_checked(broker: SyncBroker, env: MessageEnvelope) -> bool:
    """The production publish pattern: branch on the outcome, always."""
    outcome = broker.publish(env)
    if outcome.status == PublishStatus.CONFIRMED:
        print(f"  CONFIRMED  {env.routing_key}  {env.correlation_id}")
        return True
    # Not confirmed — decide what to do based on WHAT failed:
    #   RETURNED  -> unroutable (topology missing / bad routing key)
    #   NACKED    -> broker refused it (e.g. queue over max-length w/ reject)
    #   TIMEOUT   -> confirm never arrived; broker state UNKNOWN — a blind
    #                resend risks a duplicate (handlers must be idempotent)
    #   ERROR     -> transport-level; outcome.classification says how bad
    kind = outcome.classification
    print(
        f"  {outcome.status.value.upper():9}  {env.routing_key}  {env.correlation_id}"
        + (f"  ({kind.severity.value}: {type(outcome.error).__name__})" if kind else "")
    )
    return False


def main() -> int:
    cfg = RabbitConfig(
        connection=ConnectionConfig(
            host=os.environ.get("RABBITMQ_HOST", "localhost"),
            port=int(os.environ.get("RABBITMQ_PORT", "5672")),
            username=os.environ.get("RABBITMQ_USER", "guest"),
            password=os.environ.get("RABBITMQ_PASSWORD", "guest"),
            vhost=os.environ.get("RABBITMQ_VHOST", "/"),
            connection_name="production-pipeline-producer",
        ),
        publisher=PublisherConfig(confirm_delivery=True, persistent=True, confirm_timeout=5.0),
    )
    broker = SyncBroker(cfg)
    broker.start()  # publish-only: no routes registered, so no consuming

    try:
        # Wait for app.py's topology: an unroutable/failed publish is our
        # signal it isn't up yet. Probe with the first order until CONFIRMED.
        deadline = time.monotonic() + 15.0
        while not publish_checked(broker, envelope(ORDERS[0])):
            if time.monotonic() >= deadline:
                print("\ntopology not available — start app.py first, then rerun this producer.")
                return 0
            time.sleep(1.0)

        sent = 1
        for order in ORDERS[1:]:
            if publish_checked(broker, envelope(order)):
                sent += 1

        # Show RETURNED explicitly: nothing is bound to this routing key.
        print("\ndemonstrating an unroutable publish (expect RETURNED):")
        publish_checked(
            broker,
            MessageEnvelope(exchange=EXCHANGE, routing_key="order.nope", body=b"{}", mandatory=True),
        )

        print(f"\nseeded {sent}/{len(ORDERS)} orders — watch app.py's log:")
        print("  ord-1xxx  -> processed + republished to pp.orders.processed")
        print("  ord-2001  -> retried on the delay ladder, then DLQ")
        print("  ord-3001  -> straight to DLQ (permanent)")
        print("inspect the DLQ triage headers with:")
        print("  rabbitkit dlq peek pp.orders.incoming.dlq")
        return 0
    finally:
        broker.stop()


if __name__ == "__main__":
    sys.exit(main())
