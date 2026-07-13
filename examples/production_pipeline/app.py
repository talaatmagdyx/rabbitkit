"""production_pipeline/app.py — a production-grade SYNC (pika) consume+publish worker.

This is `docs/production/checklist.md` as one executable service: an order
processor that CONSUMES `order.created` events, validates and enriches them,
and PUBLISHES `order.processed` results — with every reliability decision a
production deployment needs made explicitly and commented with its "why".

What this wires up (each marked [P#] and explained inline):
  [P1] Quorum queues + broker-enforced x-delivery-limit poison backstop
  [P2] Retry ladder (delay queues) + auto-provisioned DLQ with triage headers
  [P3] Publisher confirms + persistent delivery; result publish is
       ack-after-confirmed (a lost result publish nack-requeues the source)
  [P4] Pydantic validation of message bodies (bad payload -> PERMANENT -> DLQ)
  [P5] Idempotent-handler design + optional Redis deduplication middleware
  [P6] Prometheus metrics (incl. reconnect/channel-churn counters) on :9100
  [P7] Kubernetes-correct liveness/readiness split on :8080
  [P8] Graceful SIGTERM drain + auto-reconnect via broker.run()
  [P9] Env-driven config; credentials never logged (safe_url); connection
       identified in the management UI (connection_name + client_properties)
  [P10] Bounded prefetch backpressure + a multi-thread worker pool
        (worker_count > 1 is REQUIRED here: handlers publish with confirms,
        and on a single worker that confirm wait cannot be time-bounded —
        rabbitkit warns loudly at startup if you get this wrong)

Run (terminal 1 — the worker owns ALL topology declaration):
    docker run -d --rm -p 5672:5672 rabbitmq:3.13-management-alpine
    python examples/production_pipeline/app.py

Then seed traffic (terminal 2):
    python examples/production_pipeline/producer.py

NOTE: this module deliberately does NOT use `from __future__ import
annotations` — the pipeline reads handler annotations via inspect.signature,
so `order: OrderCreated` must be a real type (not a stringized one) for
Pydantic body validation [P4] to fire.
"""

import logging
import os
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from pydantic import BaseModel, Field

from rabbitkit import (
    ConnectionConfig,
    ConsumerConfig,
    LoggingConfig,
    PublisherConfig,
    QueueType,
    RabbitConfig,
    RabbitExchange,
    RabbitQueue,
    RetryConfig,
    WorkerConfig,
)
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import ExchangeType
from rabbitkit.health import broker_liveness, broker_readiness
from rabbitkit.middleware.metrics import MetricsMiddleware
from rabbitkit.serialization import JSONSerializer
from rabbitkit.sync import SyncBroker

logger = logging.getLogger("production_pipeline")

# ── Topology constants (this worker is the single topology owner) ──────────
ORDERS_EXCHANGE = RabbitExchange(name="pp.orders", type=ExchangeType.TOPIC, durable=True)
INCOMING_QUEUE = "pp.orders.incoming"
PROCESSED_QUEUE = "pp.orders.processed"
RK_CREATED = "order.created"
RK_PROCESSED = "order.processed"


# ── [P9] Config from environment — never hardcode production credentials ───
def make_config() -> RabbitConfig:
    return RabbitConfig(
        connection=ConnectionConfig(
            host=os.environ.get("RABBITMQ_HOST", "localhost"),
            port=int(os.environ.get("RABBITMQ_PORT", "5672")),
            username=os.environ.get("RABBITMQ_USER", "guest"),
            password=os.environ.get("RABBITMQ_PASSWORD", "guest"),
            vhost=os.environ.get("RABBITMQ_VHOST", "/"),
            # heartbeat=30 (default) beats the server's 60s -> dead-peer
            # detection in ~60s. Don't raise it to "fix" slow handlers —
            # that's what worker_count > 1 is for [P10].
            # [P9] Identify this connection in the management UI. These are
            # visible in PLAINTEXT to any management-API reader — service
            # metadata only, never secrets or tenant identifiers.
            connection_name=f"production-pipeline@{os.environ.get('HOSTNAME', 'local')}",
            client_properties={
                "service_name": "production-pipeline",
                "environment": os.environ.get("ENVIRONMENT", "dev"),
            },
            # Cluster failover: RABBITMQ_NODES="rmq-1:5672,rmq-2:5672"
            nodes=tuple(n for n in os.environ.get("RABBITMQ_NODES", "").split(",") if n),
        ),
        # [P3] Confirms + persistence: publish() resolves only when the broker
        # ACKs, and messages survive a broker restart. broker.publish() never
        # raises — it returns a PublishOutcome; ALWAYS branch on it (see
        # producer.py). The @publisher result path below checks it for you:
        # an unconfirmed result publish nack-requeues the SOURCE message
        # instead of acking it into the void.
        publisher=PublisherConfig(confirm_delivery=True, persistent=True, confirm_timeout=5.0),
        # [P2] Transient failures walk this delay ladder, then dead-letter to
        # `<queue>.dlq` carrying x-rabbitkit-error-type/-error-message/
        # -first-failed-at/-last-failed-at triage headers. Demo-short delays;
        # production typically wants delays=(5, 30, 120, 600), max_retries=4.
        retry=RetryConfig(max_retries=3, delays=(2, 10, 30)),
        # [P10] Bounded prefetch = backpressure. The broker never hands this
        # worker more than PREFETCH unacked messages, so a slow downstream
        # backs up in RabbitMQ (visible, alertable) instead of in our memory.
        consumer=ConsumerConfig(
            prefetch_count=int(os.environ.get("PREFETCH_COUNT", "20")),
            graceful_timeout=30.0,  # [P8] SIGTERM drain budget; keep it above
            # your worst-case handler time, and keep Kubernetes'
            # terminationGracePeriodSeconds above THIS + preStop sleep.
        ),
        logging=LoggingConfig(render_json=os.environ.get("LOG_JSON", "0") == "1"),
    )


# ── [P6] Metrics — one shared middleware; None collector = clean no-op ─────
def make_metrics() -> MetricsMiddleware:
    try:
        from rabbitkit.middleware.metrics import PrometheusCollector

        return MetricsMiddleware(PrometheusCollector())
    except ImportError:  # prometheus-client not installed — run without metrics
        logger.warning("prometheus-client not installed; metrics disabled")
        return MetricsMiddleware(None)


def make_middlewares(metrics: MetricsMiddleware) -> list:
    """[P5] Optional Redis dedup. At-least-once delivery means handlers WILL
    occasionally run twice for one logical event (see
    docs/production/idempotency.md) — dedup narrows the window; the handler
    below is written to be safe even without it."""
    middlewares: list = [metrics]
    redis_url = os.environ.get("REDIS_URL")
    if redis_url:
        import redis

        from rabbitkit.middleware.deduplication import DeduplicationConfig, DeduplicationMiddleware

        middlewares.append(
            DeduplicationMiddleware(
                redis.from_url(redis_url),
                DeduplicationConfig(key_source="message_id", ttl=3600),
            )
        )
    return middlewares


CONFIG = make_config()
METRICS = make_metrics()
MIDDLEWARES = make_middlewares(METRICS)

# serializer= enables dict/Pydantic handler bodies (without it: raw bytes).
broker = SyncBroker(CONFIG, serializer=JSONSerializer())


# ── [P4] Message contracts — validation failures are PERMANENT -> DLQ ──────
class OrderCreated(BaseModel):
    order_id: str
    customer_id: str
    amount_cents: int = Field(gt=0)
    currency: str = Field(min_length=3, max_length=3)
    # Demo hook: "transient" raises ConnectionError (retried, then DLQ),
    # "permanent" raises ValueError (straight to DLQ). Delete in real code.
    simulate: str | None = None


@broker.subscriber(
    # [P1] Quorum queue: replicated, and delivery_limit is a BROKER-enforced
    # poison backstop — even if every client-side guard failed, RabbitMQ
    # itself dead-letters a message redelivered more than 6 times.
    queue=RabbitQueue(name=INCOMING_QUEUE, queue_type=QueueType.QUORUM, durable=True, delivery_limit=6),
    exchange=ORDERS_EXCHANGE,
    routing_key=RK_CREATED,
    middlewares=MIDDLEWARES,
)
@broker.publisher(exchange=ORDERS_EXCHANGE, routing_key=RK_PROCESSED)  # [P3] inner decorator
def process_order(order: OrderCreated, msg: RabbitMessage) -> dict:
    """Validate -> enrich -> return the result (auto-published, confirmed).

    [P5] Idempotency contract: this handler may run more than once for the
    same order (redelivery after a crash/reconnect). Everything here is safe
    to repeat — pure computation + an idempotent downstream event keyed by
    order_id. If you add side effects (charge a card, send an email), key
    them on order_id so a rerun is a no-op — retries assume this.
    """
    if order.simulate == "transient":
        raise ConnectionError(f"downstream unavailable for {order.order_id}")  # TRANSIENT -> retried
    if order.simulate == "permanent":
        raise ValueError(f"unprocessable order {order.order_id}")  # PERMANENT -> DLQ

    fee = max(30, order.amount_cents * 3 // 100)  # deterministic => rerun-safe
    logger.info(
        "processed order_id=%s amount=%d fee=%d correlation_id=%s retry=%s",
        order.order_id,
        order.amount_cents,
        fee,
        msg.correlation_id,
        msg.headers.get("x-rabbitkit-retry-count", 0),
    )
    return {
        "order_id": order.order_id,
        "customer_id": order.customer_id,
        "amount_cents": order.amount_cents,
        "fee_cents": fee,
        "currency": order.currency,
        "status": "processed",
    }


@broker.subscriber(
    queue=RabbitQueue(name=PROCESSED_QUEUE, queue_type=QueueType.QUORUM, durable=True, delivery_limit=6),
    exchange=ORDERS_EXCHANGE,
    routing_key=RK_PROCESSED,
    middlewares=MIDDLEWARES,
)
def record_processed(event: dict, msg: RabbitMessage) -> None:
    """Downstream sink — closes the loop so the demo is observable end-to-end."""
    logger.info("recorded %s (correlation_id=%s)", event.get("order_id"), msg.correlation_id)


# ── [P7] Health: liveness and readiness are DIFFERENT questions ────────────
def start_health_server(port: int) -> None:
    """Liveness = process alive & not wedged (ignores broker state — a broker
    outage must NOT restart the fleet). Readiness = connected + not blocked +
    consumers actually active. rabbitkit's health functions are explicitly
    safe to call from this probe thread (they only READ broker state — the
    owner-thread rule applies to transport operations, not health checks).
    See docs/kubernetes.md for probe manifests."""

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # stdlib API name
            if self.path == "/healthz/live":
                ok, body = broker_liveness(broker), b'{"live": true}'
            elif self.path == "/healthz/ready":
                ok, body = broker_readiness(broker), b'{"ready": true}'
            else:
                self.send_response(404)
                self.end_headers()
                return
            if not ok:
                body = body.replace(b"true", b"false")
            self.send_response(200 if ok else 503)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, *_args: object) -> None:  # probes are noisy
            pass

    # Probe endpoints must be reachable by the kubelet -> all interfaces
    # (deliberate; they expose no data beyond up/down).
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)  # noqa: S104
    threading.Thread(target=server.serve_forever, name="health-server", daemon=True).start()
    logger.info("health endpoints on :%d (/healthz/live, /healthz/ready)", port)


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    # [P9] safe_url masks the password — never log ConnectionConfig.url.
    logger.info("connecting to %s", CONFIG.connection.safe_url)

    if METRICS.collector is not None:
        from rabbitkit.middleware.metrics import start_metrics_server

        # Loopback by default; bind 0.0.0.0 explicitly for k8s scrapers and
        # gate it with a NetworkPolicy (the endpoint is unauthenticated).
        start_metrics_server(port=int(os.environ.get("METRICS_PORT", "9100")))

    start_health_server(int(os.environ.get("HEALTH_PORT", "8080")))

    # [P8]+[P10] run() blocks: start + consume + reconnect-on-drop + SIGTERM
    # drain. worker_count > 1 moves handlers OFF the connection's I/O thread:
    # slow handlers can't starve heartbeats, and handler publishes (our
    # confirmed result publish!) go through the bounded cross-thread path —
    # on a single worker that confirm wait cannot be time-bounded (rabbitkit
    # emits a RuntimeWarning at startup if you try).
    broker.run(worker_config=WorkerConfig(worker_count=int(os.environ.get("WORKER_COUNT", "4"))))


if __name__ == "__main__":
    main()
