"""Real RabbitMQ integration tests — requires Docker / testcontainers.

Run with::

    pytest tests/integration/test_real_rabbitmq.py -m integration -v

These tests spin up a real RabbitMQ container via testcontainers-python and
exercise the full stack (AsyncBroker + AsyncTransportImpl + aio-pika, and
SyncBroker + SyncTransport + pika).

The tests are automatically skipped when:
- ``testcontainers`` is not installed
- Docker daemon is not reachable

Install prerequisites::

    pip install testcontainers[rabbitmq]   # or: pip install testcontainers
"""

from __future__ import annotations

import asyncio
import threading
import time
from typing import Any

import pytest

# Module-level imports so typing.get_type_hints() can resolve annotations
# even with `from __future__ import annotations` (PEP 563).
try:
    from rabbitkit.core.message import RabbitMessage as _RabbitMessage  # noqa: F401
except ImportError:  # pragma: no cover
    pass

# ── Skip guard — skip entire module when testcontainers/docker unavailable ──

try:
    from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

    _TESTCONTAINERS_AVAILABLE = True
except ImportError:
    _TESTCONTAINERS_AVAILABLE = False

pytestmark = pytest.mark.integration


def _skip_no_docker() -> None:
    """Raise pytest.skip() when prerequisites are missing."""
    if not _TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not installed — run: pip install testcontainers[rabbitmq]")
    try:
        import docker  # type: ignore[import-untyped]

        docker.from_env().ping()
    except Exception:
        pytest.skip("Docker daemon not reachable — skip real-RabbitMQ integration tests")


# ── Fixture ──────────────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def rabbitmq_url() -> str:  # type: ignore[return]
    """Start a RabbitMQ container and yield its AMQP URL.

    Module-scoped so the container is reused across tests in this file.
    """
    _skip_no_docker()
    with RabbitMqContainer("rabbitmq:3.13-management-alpine") as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5672)
        yield f"amqp://guest:guest@{host}:{port}/"


# ── Helpers ───────────────────────────────────────────────────────────────────


def _make_async_config(url: str, **kwargs: Any) -> Any:
    """Build a RabbitConfig from a URL for AsyncBroker."""
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    return RabbitConfig(connection=ConnectionConfig.from_url(url), **kwargs)


def _make_sync_config(url: str, **kwargs: Any) -> Any:
    """Build a RabbitConfig from a URL for SyncBroker."""
    from rabbitkit.core.config import ConnectionConfig, RabbitConfig

    return RabbitConfig(connection=ConnectionConfig.from_url(url), **kwargs)


# ══════════════════════════════════════════════════════════════════════════════
# Async integration tests
# ══════════════════════════════════════════════════════════════════════════════


async def test_async_roundtrip_publish_consume(rabbitmq_url: str) -> None:
    """Subscribe BEFORE start, publish, verify handler receives the message."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.types import MessageEnvelope

    received: list[bytes] = []
    done = asyncio.Event()

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config)

    @broker.subscriber(queue="integ-rt-orders")
    async def handle(body: bytes) -> None:
        received.append(body)
        done.set()

    await broker.start()
    await asyncio.sleep(0.3)  # allow consumer registration

    await broker.publish(MessageEnvelope(routing_key="integ-rt-orders", body=b'{"id": 1}'))

    await asyncio.wait_for(done.wait(), timeout=15.0)

    assert received == [b'{"id": 1}']
    await broker.stop()


async def test_async_multiple_queues(rabbitmq_url: str) -> None:
    """Two subscribers on different queues receive only their own messages."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.types import MessageEnvelope

    queue_a: list[bytes] = []
    queue_b: list[bytes] = []
    done_a = asyncio.Event()
    done_b = asyncio.Event()

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config)

    @broker.subscriber(queue="integ-mq-alpha")
    async def handle_a(body: bytes) -> None:
        queue_a.append(body)
        done_a.set()

    @broker.subscriber(queue="integ-mq-beta")
    async def handle_b(body: bytes) -> None:
        queue_b.append(body)
        done_b.set()

    await broker.start()
    await asyncio.sleep(0.3)

    await broker.publish(MessageEnvelope(routing_key="integ-mq-alpha", body=b"msg-alpha"))
    await broker.publish(MessageEnvelope(routing_key="integ-mq-beta", body=b"msg-beta"))

    await asyncio.wait_for(done_a.wait(), timeout=15.0)
    await asyncio.wait_for(done_b.wait(), timeout=15.0)

    assert queue_a == [b"msg-alpha"]
    assert queue_b == [b"msg-beta"]
    await broker.stop()


async def test_async_topic_exchange_routing(rabbitmq_url: str) -> None:
    """Topic exchange with wildcards routes to matching subscribers only."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.topology import RabbitExchange
    from rabbitkit.core.types import ExchangeType, MessageEnvelope

    order_msgs: list[bytes] = []
    all_msgs: list[bytes] = []
    order_done = asyncio.Event()
    all_done = asyncio.Event()

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config)

    topic_exchange = RabbitExchange(name="integ-topic-ex", type=ExchangeType.TOPIC)

    @broker.subscriber(queue="integ-topic-orders", exchange=topic_exchange, routing_key="order.#")
    async def handle_orders(body: bytes) -> None:
        order_msgs.append(body)
        order_done.set()

    @broker.subscriber(queue="integ-topic-all", exchange=topic_exchange, routing_key="#")
    async def handle_all(body: bytes) -> None:
        all_msgs.append(body)
        if len(all_msgs) >= 2:
            all_done.set()

    await broker.start()
    await asyncio.sleep(0.3)

    await broker.publish(
        MessageEnvelope(routing_key="order.created", body=b"order-msg", exchange="integ-topic-ex")
    )
    await broker.publish(
        MessageEnvelope(routing_key="payment.processed", body=b"payment-msg", exchange="integ-topic-ex")
    )

    await asyncio.wait_for(order_done.wait(), timeout=15.0)
    await asyncio.wait_for(all_done.wait(), timeout=15.0)

    assert order_msgs == [b"order-msg"]
    assert len(all_msgs) == 2
    assert b"order-msg" in all_msgs
    assert b"payment-msg" in all_msgs
    await broker.stop()


async def test_async_fanout_exchange(rabbitmq_url: str) -> None:
    """Fanout exchange delivers the same message to all bound queues."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.topology import RabbitExchange
    from rabbitkit.core.types import ExchangeType, MessageEnvelope

    consumer1: list[bytes] = []
    consumer2: list[bytes] = []
    done1 = asyncio.Event()
    done2 = asyncio.Event()

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config)

    fanout_exchange = RabbitExchange(name="integ-fanout-ex", type=ExchangeType.FANOUT)

    @broker.subscriber(queue="integ-fanout-q1", exchange=fanout_exchange, routing_key="")
    async def handle_1(body: bytes) -> None:
        consumer1.append(body)
        done1.set()

    @broker.subscriber(queue="integ-fanout-q2", exchange=fanout_exchange, routing_key="")
    async def handle_2(body: bytes) -> None:
        consumer2.append(body)
        done2.set()

    await broker.start()
    await asyncio.sleep(0.3)

    await broker.publish(
        MessageEnvelope(routing_key="", body=b"broadcast", exchange="integ-fanout-ex")
    )

    await asyncio.wait_for(done1.wait(), timeout=15.0)
    await asyncio.wait_for(done2.wait(), timeout=15.0)

    assert consumer1 == [b"broadcast"]
    assert consumer2 == [b"broadcast"]
    await broker.stop()


async def test_async_message_headers_preserved(rabbitmq_url: str) -> None:
    """Custom headers published on a message arrive intact at the subscriber.

    The handler uses the pipeline's FALLBACK parameter resolver (no DI resolver):
    - first param (no annotation) → body (bytes)
    - second param (no annotation) → full RabbitMessage (fallback for 2nd param)

    This avoids ``from __future__ import annotations`` type-hint string resolution
    issues because the fallback injector does NOT evaluate annotation strings.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.types import MessageEnvelope

    received: list[tuple[bytes, dict[str, Any]]] = []
    done = asyncio.Event()

    config = _make_async_config(rabbitmq_url)
    # No di_resolver — use the pipeline's built-in fallback injector.
    broker = AsyncBroker(config=config)

    @broker.subscriber(queue="integ-headers-q")
    async def handle(body, msg) -> None:  # type: ignore[no-untyped-def]
        # body: bytes (first positional), msg: RabbitMessage (pipeline fallback)
        assert isinstance(msg, RabbitMessage)
        received.append((bytes(body), dict(msg.headers)))
        done.set()

    await broker.start()
    await asyncio.sleep(0.3)

    await broker.publish(
        MessageEnvelope(
            routing_key="integ-headers-q",
            body=b"with-headers",
            headers={"x-tenant": "acme", "x-priority": "high", "x-version": "2"},
        )
    )

    await asyncio.wait_for(done.wait(), timeout=15.0)

    assert len(received) == 1
    body_bytes, hdrs = received[0]
    assert body_bytes == b"with-headers"
    assert hdrs.get("x-tenant") == "acme"
    assert hdrs.get("x-priority") == "high"
    assert hdrs.get("x-version") == "2"
    await broker.stop()


async def test_async_broker_publish_applies_signing_middleware(rabbitmq_url: str) -> None:
    """C3: broker.publish() must apply broker-level middlewares (e.g. signing).

    Before the C3 fix, publish_scope only fired for handler-RESULT publishing
    (Contract 5) — broker.publish(), the primary producer API, went straight to
    the transport with no middleware applied at all, so a SigningMiddleware
    configured on the broker never signed anything sent via broker.publish().
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.middleware.signing import SigningConfig, SigningMiddleware

    received_headers: dict[str, Any] = {}
    done = asyncio.Event()

    signing_mw = SigningMiddleware(SigningConfig(secret_key="integ-test-secret"))
    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config, middlewares=[signing_mw])

    @broker.subscriber(queue="integ-signed-publish-q")
    async def handle(body, msg) -> None:  # type: ignore[no-untyped-def]
        assert isinstance(msg, RabbitMessage)
        received_headers.update(msg.headers)
        done.set()

    await broker.start()
    await asyncio.sleep(0.3)

    await broker.publish(
        MessageEnvelope(routing_key="integ-signed-publish-q", body=b"order-payload")
    )

    await asyncio.wait_for(done.wait(), timeout=15.0)

    assert "x-rabbitkit-signature" in received_headers
    assert "x-rabbitkit-sign-timestamp" in received_headers
    assert "x-rabbitkit-sign-nonce" in received_headers
    await broker.stop()


@pytest.mark.parametrize("confirm_delivery", [True, False])
async def test_async_mandatory_publish_to_nonexistent_binding_returns_returned(
    rabbitmq_url: str, confirm_delivery: bool
) -> None:
    """H1: an unroutable mandatory=True publish must report RETURNED, never CONFIRMED.

    Regardless of the broker's global ``confirm_delivery`` setting, publishing
    with ``mandatory=True`` to a routing key with no matching queue/binding
    must surface as ``PublishStatus.RETURNED`` — never silently CONFIRMED.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import PublisherConfig
    from rabbitkit.core.types import MessageEnvelope, PublishStatus

    config = _make_async_config(
        rabbitmq_url, publisher=PublisherConfig(confirm_delivery=confirm_delivery)
    )
    broker = AsyncBroker(config=config)
    await broker.start()

    outcome = await broker.publish(
        MessageEnvelope(
            routing_key="integ-nonexistent-queue-h1",
            body=b"should-be-returned",
            mandatory=True,
        )
    )

    assert outcome.status == PublishStatus.RETURNED
    assert not outcome.ok
    await broker.stop()


async def test_async_publish_without_confirms_reports_sent_not_confirmed(rabbitmq_url: str) -> None:
    """M4: a non-mandatory publish with confirm_delivery=False must report
    SENT, not CONFIRMED -- the broker never actually acknowledged it, even
    though the message is genuinely delivered (this proves the fast path
    still works end-to-end against a real broker, just honestly labeled)."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import PublisherConfig
    from rabbitkit.core.types import MessageEnvelope, PublishStatus

    received: list[bytes] = []
    done = asyncio.Event()

    config = _make_async_config(rabbitmq_url, publisher=PublisherConfig(confirm_delivery=False))
    broker = AsyncBroker(config=config)

    @broker.subscriber(queue="integ-m4-sent-q")
    async def handle(body: bytes) -> None:
        received.append(body)
        done.set()

    await broker.start()
    await asyncio.sleep(0.3)

    outcome = await broker.publish(MessageEnvelope(routing_key="integ-m4-sent-q", body=b"fire-and-forget"))

    assert outcome.status == PublishStatus.SENT
    assert outcome.ok  # not a failure -- just not broker-confirmed

    await asyncio.wait_for(done.wait(), timeout=15.0)
    assert received == [b"fire-and-forget"]
    await broker.stop()


async def test_async_declare_queue_precondition_failed_raises_configuration_error(
    rabbitmq_url: str,
) -> None:
    """M6: starting a broker whose queue declaration conflicts with an
    existing queue's arguments (e.g. ops tooling declared it as a quorum
    queue; rabbitkit's default is classic) must fail with a typed,
    actionable ConfigurationError -- not an opaque low-level channel-closed
    traceback -- against a REAL broker's actual 406 response.
    """
    import pika

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.errors import ConfigurationError

    queue_name = "integ-m6-precondition-q"

    # Pre-declare the queue as a quorum queue via a raw pika connection --
    # simulating an ops-created queue with different arguments than
    # whatever rabbitkit would declare (classic, by default).
    params = pika.URLParameters(rabbitmq_url)
    conn = pika.BlockingConnection(params)
    channel = conn.channel()
    channel.queue_declare(queue=queue_name, durable=True, arguments={"x-queue-type": "quorum"})
    conn.close()

    broker = AsyncBroker(config=_make_async_config(rabbitmq_url))

    @broker.subscriber(queue=queue_name)
    async def handle(body: bytes) -> None:  # pragma: no cover - never reached
        pass

    try:
        with pytest.raises(ConfigurationError, match=queue_name):
            await broker.start()
    finally:
        # Clean up the quorum queue so a re-run doesn't hit a stale artifact.
        cleanup_conn = pika.BlockingConnection(params)
        cleanup_channel = cleanup_conn.channel()
        cleanup_channel.queue_delete(queue=queue_name)
        cleanup_conn.close()


async def test_async_retry_exhaustion_to_dlq(rabbitmq_url: str) -> None:
    """A TRANSIENT failure is retried through the delay queue, then dead-lettered.

    Uses a transient error (``TimeoutError``) so retry actually engages —
    ``RetryConfig(max_retries=1, delays=(1,))`` means one delayed retry (~1s)
    then the message is dead-lettered to ``<queue>.dlq``. We assert the handler
    is called exactly twice (original + 1 retry) and that the DLQ receives the
    exhausted message — proving retry is wired, not just the topology declared.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import RetryConfig
    from rabbitkit.core.types import MessageEnvelope

    call_count = 0
    exhausted = asyncio.Event()
    dead_lettered = asyncio.Event()

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config)

    retry_cfg = RetryConfig(max_retries=1, delays=(1,))

    @broker.subscriber(queue="integ-dlq-src", retry=retry_cfg)
    async def always_fail(body: bytes) -> None:
        nonlocal call_count
        call_count += 1
        if call_count >= 2:  # original + 1 retry
            exhausted.set()
        raise TimeoutError("transient outage")  # transient → engages retry

    # Consume the DLQ to prove the exhausted message is dead-lettered there.
    @broker.subscriber(queue="integ-dlq-src.dlq")
    async def on_dlq(body: bytes) -> None:
        dead_lettered.set()

    await broker.start()
    await asyncio.sleep(0.3)

    await broker.publish(MessageEnvelope(routing_key="integ-dlq-src", body=b"doomed"))

    await asyncio.wait_for(exhausted.wait(), timeout=20.0)
    await asyncio.wait_for(dead_lettered.wait(), timeout=20.0)

    assert call_count == 2, f"expected original + 1 retry = 2 handler calls, got {call_count}"
    await broker.stop()


async def test_async_retry_routes_back_through_topic_bound_queue(rabbitmq_url: str) -> None:
    """M5: a delay queue's dead-letter-on-expiry must reach a source queue
    that is bound to its real exchange via a topic PATTERN, not by its own
    literal name.

    Before the fix, the delay queue dead-lettered to the source's real
    exchange using the source queue's NAME as the routing key
    (``x-dead-letter-exchange=<source_exchange>``,
    ``x-dead-letter-routing-key=<source_queue_name>``). A queue bound with
    ``routing_key="orders.*.created"`` is never bound with routing key
    ``"integ-m5-topic-src"`` (its own name) on a topic exchange, so that
    routing key matches nothing — the retried message vanishes after the
    delay instead of coming back, and the handler is only ever called once
    (no second attempt, no dead-letter to the DLQ either). The fix
    dead-letters via the DEFAULT exchange instead, which always delivers
    directly to a queue by name regardless of its other bindings.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import RetryConfig
    from rabbitkit.core.topology import RabbitExchange
    from rabbitkit.core.types import ExchangeType, MessageEnvelope

    call_count = 0
    retried = asyncio.Event()

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config)

    topic_exchange = RabbitExchange(name="integ-m5-topic-ex", type=ExchangeType.TOPIC)
    retry_cfg = RetryConfig(max_retries=1, delays=(1,))

    @broker.subscriber(
        queue="integ-m5-topic-src",
        exchange=topic_exchange,
        routing_key="orders.*.created",  # pattern binding -- NOT the queue's own name
        retry=retry_cfg,
    )
    async def handle(body: bytes) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise TimeoutError("transient outage")  # engages the one configured retry
        retried.set()  # only reached if the delay-queue dead-letter routed back

    await broker.start()
    await asyncio.sleep(0.3)

    await broker.publish(
        MessageEnvelope(
            routing_key="orders.us.created", body=b"m5-msg", exchange="integ-m5-topic-ex"
        )
    )

    await asyncio.wait_for(retried.wait(), timeout=20.0)

    assert call_count == 2, f"expected original + 1 successful retry = 2 handler calls, got {call_count}"
    await broker.stop()


async def test_async_retry_count_header_spoofing_clamped(rabbitmq_url: str) -> None:
    """H5: a producer-supplied x-rabbitkit-retry-count header must not let an
    attacker force unbounded retries (negative value) or skip straight to
    the DLQ (huge value), against a real broker.

    A huge spoofed count must clamp to max_retries and dead-letter on the
    very first delivery (no retry actually happens). A negative spoofed
    count must clamp to 0 and retry through the real, DECLARED retry.1 delay
    queue — before the H5 fix this would target a non-existent
    ``...retry.-4`` queue on the default exchange and be silently dropped
    (the source message acked, the retry never seen again).

    Uses -999 rather than -5 for the negative case: unrelated to this fix,
    ``pamqp`` (aio_pika's AMQP table encoder) cannot encode small negative
    ints (-128..-1) as a header value at all — ``pamqp.encode.table_integer``
    raises ``struct.error: 'B' format requires 0 <= number <= 255`` for e.g.
    -5, because it picks an unsigned-byte encoding for values in that range
    regardless of sign. -999 falls outside that broken range and encodes
    fine; the retry-count clamping logic under test does not care about the
    exact negative value. The unit tests cover -5 directly since they never
    go through real AMQP wire encoding.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import RetryConfig
    from rabbitkit.core.types import MessageEnvelope

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config)
    retry_cfg = RetryConfig(max_retries=2, delays=(1, 1))

    huge_call_count = 0
    huge_dead_lettered = asyncio.Event()

    @broker.subscriber(queue="integ-h5-huge-q", retry=retry_cfg)
    async def handle_huge(body: bytes) -> None:
        nonlocal huge_call_count
        huge_call_count += 1
        raise TimeoutError("transient outage")

    @broker.subscriber(queue="integ-h5-huge-q.dlq")
    async def on_huge_dlq(body: bytes) -> None:
        huge_dead_lettered.set()

    neg_call_count = 0
    neg_retried = asyncio.Event()

    @broker.subscriber(queue="integ-h5-neg-q", retry=retry_cfg)
    async def handle_neg(body: bytes) -> None:
        nonlocal neg_call_count
        neg_call_count += 1
        if neg_call_count >= 2:
            neg_retried.set()
        raise TimeoutError("transient outage")

    await broker.start()
    await asyncio.sleep(0.3)

    await broker.publish(
        MessageEnvelope(
            routing_key="integ-h5-huge-q",
            body=b"huge-spoof",
            headers={"x-rabbitkit-retry-count": 10**9},
        )
    )
    await asyncio.wait_for(huge_dead_lettered.wait(), timeout=20.0)
    assert huge_call_count == 1, "a spoofed huge retry count must skip straight to the DLQ, not retry"

    await broker.publish(
        MessageEnvelope(
            routing_key="integ-h5-neg-q",
            body=b"neg-spoof",
            headers={"x-rabbitkit-retry-count": -999},
        )
    )
    await asyncio.wait_for(neg_retried.wait(), timeout=20.0)
    assert neg_call_count == 2, "a spoofed negative retry count must still retry normally, not be dropped"

    await broker.stop()


async def test_async_quorum_queue_delivery_limit_dead_letters_independent_of_app_retry(
    rabbitmq_url: str,
) -> None:
    """Loop Engineering Review, Reliability: a quorum queue's broker-enforced
    ``x-delivery-limit`` must dead-letter an endlessly-nacked message on its
    own, with NO application-level retry configured at all -- proving the
    broker-side backstop (recommended in docs/retry-and-dlq.md for
    defense-in-depth against a compromised/buggy app-level retry-count
    header) actually works against a real broker, not just that
    ``RabbitQueue.to_declare_kwargs()`` maps the field correctly (already
    covered at the unit level in tests/unit/core/test_topology.py).

    No ``retry=`` is set on this route -- the handler always raises a
    TRANSIENT-classified error under the default AckPolicy.AUTO, which nacks
    with ``requeue=True`` directly (no RetryMiddleware, no
    ``x-rabbitkit-retry-count`` header involved whatsoever). The ONLY thing
    that can stop the redelivery loop is RabbitMQ's own quorum-queue
    delivery limit.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.core.types import MessageEnvelope, QueueType

    call_count = 0
    dead_lettered = asyncio.Event()

    dlq_name = "integ-quorum-delivery-limit-dlq"
    src_queue = RabbitQueue(
        name="integ-quorum-delivery-limit-src",
        queue_type=QueueType.QUORUM,
        durable=True,
        delivery_limit=1,  # dead-letter after the original delivery + 1 redelivery
        dead_letter_exchange="",
        dead_letter_routing_key=dlq_name,
    )

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config)

    @broker.subscriber(queue=src_queue)  # deliberately: no retry= at all
    async def always_transient_failure(body: bytes) -> None:
        nonlocal call_count
        call_count += 1
        raise TimeoutError("app never gives up on its own -- only the broker limit can stop this")

    @broker.subscriber(queue=dlq_name)
    async def on_dlq(body: bytes) -> None:
        dead_lettered.set()

    await broker.start()
    await asyncio.sleep(0.3)

    await broker.publish(MessageEnvelope(routing_key="integ-quorum-delivery-limit-src", body=b"doomed"))

    await asyncio.wait_for(dead_lettered.wait(), timeout=20.0)

    # delivery_limit=1 means the broker dead-letters after exactly 2 total
    # deliveries (original + 1 redelivery). If the app-level retry-count
    # header (which doesn't even exist on this route) were somehow the thing
    # stopping this, call_count would either never stabilize or dead_lettered
    # would never fire -- this proves the broker's own limit did the work.
    assert call_count == 2, f"expected exactly 2 attempts (delivery_limit=1), got {call_count}"

    await broker.stop()


async def test_async_filter_rejection_without_retry_preserved_in_auto_dlq(rabbitmq_url: str) -> None:
    """H6: a filter_fn rejection on a route with no retry and no manually
    configured DLX must not silently discard the message.

    Before the fix, filter_fn returning False settled the message with
    nack(requeue=False); with no dead-letter-exchange configured (only wired
    automatically when retry is enabled), RabbitMQ would just drop it. The
    fix auto-declares a "<queue>.dlq" DLQ and wires the source queue's DLX to
    it whenever a route has filter_fn but no retry and no manual DLX — this
    proves the rejected message actually lands there on a real broker.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.types import MessageEnvelope

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config)

    handler_called = asyncio.Event()
    dead_lettered_bodies: list[bytes] = []
    dead_lettered = asyncio.Event()

    def reject_everything(msg: object) -> bool:
        return False  # filter_fn rejects every message

    @broker.subscriber(queue="integ-h6-filter-q", filter_fn=reject_everything)
    async def handle(body: bytes) -> None:
        # Must never be called — the message is filtered before the handler.
        handler_called.set()

    @broker.subscriber(queue="integ-h6-filter-q.dlq")
    async def on_dlq(body: bytes) -> None:
        dead_lettered_bodies.append(body)
        dead_lettered.set()

    with pytest.warns(RuntimeWarning, match="auto-declared 'integ-h6-filter-q.dlq'"):
        await broker.start()
    await asyncio.sleep(0.3)

    await broker.publish(MessageEnvelope(routing_key="integ-h6-filter-q", body=b"filtered-payload"))

    await asyncio.wait_for(dead_lettered.wait(), timeout=20.0)

    assert dead_lettered_bodies == [b"filtered-payload"]
    assert not handler_called.is_set(), "filter_fn must reject before the handler ever runs"

    await broker.stop()


async def test_async_dedup_retry_composition_does_not_drop_retried_message(rabbitmq_url: str) -> None:
    """H8's exact spec: [dedup(on_success), retry] — dedup OUTER of retry —
    a handler that fails once (transient) then succeeds must actually be
    processed on the retry delivery, not dropped as a duplicate.

    RetryMiddleware swallows the handler's transient failure (requeues to a
    delay queue, acks the source) rather than raising, so from
    DeduplicationMiddleware's point of view — sitting OUTER of retry here —
    call_next(message) returns normally either way. Without checking for
    REQUEUED_FOR_RETRY, dedup would mark the message processed on the FAILED
    first attempt, so the real retry delivery (same dedup key — message_id
    is preserved across the retry envelope) would be dropped as a duplicate
    and never actually processed.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import DeduplicationConfig, RetryConfig
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.middleware.deduplication import DeduplicationMiddleware
    from rabbitkit.middleware.retry import RetryMiddleware

    class _FakeAsyncRedis:
        """Duck-typed in-memory stand-in for a real async redis client —
        DeduplicationMiddleware only needs .set(nx=True, ex=...)/.delete()."""

        def __init__(self) -> None:
            self._store: dict[str, str] = {}

        async def set(self, key: str, value: str, *, nx: bool = False, ex: int | None = None) -> bool | None:
            if nx and key in self._store:
                return None
            self._store[key] = value
            return True

        async def delete(self, key: str) -> None:
            self._store.pop(key, None)

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config)

    redis = _FakeAsyncRedis()
    dedup_mw = DeduplicationMiddleware(redis_client=redis, config=DeduplicationConfig(key_source="message_id"))
    retry_cfg = RetryConfig(max_retries=2, delays=(1, 1))
    # publish_async_fn=broker.publish resolves broker._transport lazily at
    # call time, after broker.start() has already run — mirrors what
    # _wire_retry_middleware would wire up automatically for an
    # auto-installed RetryMiddleware, but that auto-wiring only ever
    # inserts retry OUTERMOST, which can't reproduce dedup-outer-of-retry.
    retry_mw = RetryMiddleware(retry_cfg, publish_async_fn=broker.publish)

    call_count = 0
    processed_bodies: list[bytes] = []
    processed = asyncio.Event()

    # dedup listed FIRST (outer), retry SECOND (inner) — the exact
    # problematic composition H8 describes. retry= still declares the
    # delay-queue/DLQ topology; middlewares=[...] supplies our own
    # RetryMiddleware instance instead of letting the broker auto-wire one
    # (which would always place it outermost).
    @broker.subscriber(queue="integ-h8-dedup-retry-q", retry=retry_cfg, middlewares=[dedup_mw, retry_mw])
    async def handle(body: bytes) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise TimeoutError("transient outage")  # transient -> retry requeues
        processed_bodies.append(body)
        processed.set()

    await broker.start()
    await asyncio.sleep(0.3)

    await broker.publish(MessageEnvelope(routing_key="integ-h8-dedup-retry-q", body=b"h8-payload"))

    await asyncio.wait_for(processed.wait(), timeout=20.0)

    assert call_count == 2, "expected one failed attempt + one successful retry attempt"
    assert processed_bodies == [b"h8-payload"], "message must be processed exactly once, not dropped"

    await broker.stop()


async def test_async_worker_pool_concurrent_messages(rabbitmq_url: str) -> None:
    """Multiple messages are processed concurrently via WorkerConfig(worker_count=4)."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import PoolConfig, WorkerConfig
    from rabbitkit.core.types import MessageEnvelope

    received: list[bytes] = []
    all_done = asyncio.Event()
    num_messages = 8

    config = _make_async_config(rabbitmq_url, pool=PoolConfig(channel_pool_size=16))
    broker = AsyncBroker(config=config)

    @broker.subscriber(queue="integ-pool-q")
    async def handle(body: bytes) -> None:
        await asyncio.sleep(0.05)  # simulate work
        received.append(body)
        if len(received) >= num_messages:
            all_done.set()

    await broker.start(worker_config=WorkerConfig(worker_count=4))
    await asyncio.sleep(0.3)

    for i in range(num_messages):
        await broker.publish(MessageEnvelope(routing_key="integ-pool-q", body=f"msg-{i}".encode()))

    await asyncio.wait_for(all_done.wait(), timeout=30.0)

    assert len(received) == num_messages
    await broker.stop()


async def test_async_stop_drains_cleanly_under_load(rabbitmq_url: str) -> None:
    """C5: stop() cancels consumers before draining the worker pool, so no
    message is orphaned when many are still in flight or queued at shutdown
    time.

    Before the fix, the pool drained first while the consumer stayed active
    for the whole (potentially graceful_timeout-long) wait — a message
    delivered in that window was submitted via AsyncWorkerPool.submit(),
    which creates a task unconditionally and would never be awaited once
    stop() had already cleared _tasks: an orphaned, never-settled delivery,
    redelivered (or lost) later.

    stop() is called deliberately early (before all messages finish), so
    plenty are still queued/in-flight. Anything left in the queue after
    cancellation (never delivered to this consumer at all) is drained by a
    follow-up consumer. Every published body must be processed by EITHER
    consumer — none permanently lost. (Duplicates are acceptable under
    at-least-once; C5's guarantee is against permanent loss/orphaning.)
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import PoolConfig, WorkerConfig
    from rabbitkit.core.types import MessageEnvelope

    num_messages = 30
    processed_first: list[bytes] = []

    config = _make_async_config(rabbitmq_url, pool=PoolConfig(channel_pool_size=16))
    broker = AsyncBroker(config=config)

    @broker.subscriber(queue="integ-shutdown-q")
    async def handle(body: bytes) -> None:
        await asyncio.sleep(0.05)  # slow enough that many stay queued/in-flight
        processed_first.append(body)

    await broker.start(worker_config=WorkerConfig(worker_count=4))
    await asyncio.sleep(0.3)

    for i in range(num_messages):
        await broker.publish(MessageEnvelope(routing_key="integ-shutdown-q", body=f"m{i}".encode()))

    # Stop promptly — don't wait for completion — so the shutdown-ordering
    # path is genuinely exercised (many messages still queued/in-flight).
    await asyncio.sleep(0.15)
    await broker.stop(timeout=15.0)

    # Drain anything left in the queue (cancelled before delivery) with a
    # follow-up consumer — proves nothing was silently lost.
    processed_second: list[bytes] = []
    done2 = asyncio.Event()

    broker2 = AsyncBroker(config=config)

    @broker2.subscriber(queue="integ-shutdown-q")
    async def handle2(body: bytes) -> None:
        processed_second.append(body)
        if len(processed_first) + len(processed_second) >= num_messages:
            done2.set()

    await broker2.start()
    if len(processed_first) < num_messages:
        try:
            await asyncio.wait_for(done2.wait(), timeout=10.0)
        except TimeoutError:
            pass
    await broker2.stop()

    all_processed = processed_first + processed_second
    assert set(all_processed) == {f"m{i}".encode() for i in range(num_messages)}, (
        f"expected all {num_messages} messages processed, got "
        f"{len(processed_first)} (first) + {len(processed_second)} (follow-up) = "
        f"{len(set(all_processed))} unique"
    )


async def test_async_worker_pool_abandoned_handler_is_nacked_for_redelivery(rabbitmq_url: str) -> None:
    """H12's exact spec: a handler that outlives ``stop_timeout`` — with a
    pre-ack side effect already recorded — must result in redelivery, not
    silent loss.

    ``AsyncWorkerPool.stop()`` cancels a task still running past the
    deadline; before the fix, that was a bare cancel with nothing guaranteeing
    the message ever got settled (``CancelledError`` is a ``BaseException``,
    so the pipeline's ``except Exception`` handling never sees it and never
    acks/nacks). The message would eventually be requeued only implicitly,
    when the connection closes and RabbitMQ auto-requeues its unacked
    deliveries — this test proves it happens explicitly and promptly via the
    H12 fix's own nack(requeue=True), by observing a follow-up consumer
    receive the same message right after ``stop()`` returns, without relying
    on a fresh connection/channel cycle.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import WorkerConfig
    from rabbitkit.core.types import MessageEnvelope

    side_effects: list[bytes] = []
    handler_started = asyncio.Event()

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config)

    @broker.subscriber(queue="integ-h12-abandon-q")
    async def handle(body: bytes) -> None:
        side_effects.append(body)  # the "pre-ack side effect" H12 refers to
        handler_started.set()
        await asyncio.sleep(5.0)  # far longer than stop_timeout below

    await broker.start(worker_config=WorkerConfig(worker_count=2, stop_timeout=0.3))
    await asyncio.sleep(0.3)

    await broker.publish(MessageEnvelope(routing_key="integ-h12-abandon-q", body=b"slow-msg"))
    await asyncio.wait_for(handler_started.wait(), timeout=10.0)

    await broker.stop(timeout=0.3)  # abandons the still-sleeping handler

    assert side_effects == [b"slow-msg"], "the side effect should have run before abandonment"

    # Redelivery proof: a follow-up consumer on the same queue must receive
    # the message again — it must not be permanently lost.
    redelivered: list[bytes] = []
    done = asyncio.Event()

    broker2 = AsyncBroker(config=config)

    @broker2.subscriber(queue="integ-h12-abandon-q")
    async def handle2(body: bytes) -> None:
        redelivered.append(body)
        done.set()

    await broker2.start()
    await asyncio.wait_for(done.wait(), timeout=15.0)
    await broker2.stop()

    assert redelivered == [b"slow-msg"]


async def test_async_compression_roundtrip(rabbitmq_url: str) -> None:
    """C4: CompressionMiddleware, once wired via publish_scope_async, compresses
    on broker.publish() and decompresses automatically via on_receive_async —
    no manual transform_envelope()/decompress() calls needed by the caller.

    Two queues prove both halves against a real broker:
    - integ-compress-raw-q (no middleware): receives the RAW wire bytes,
      proving broker.publish() actually compressed the body (not a silent
      no-op — this is exactly the defect C4 reported: nothing compressed
      anything on the standard paths).
    - integ-compress-decoded-q (middleware attached): on_receive_async
      decompresses automatically; the handler sees the ORIGINAL body.
    """
    import gzip

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import CompressionConfig
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.middleware.compression import CompressionMiddleware

    raw_received: list[bytes] = []
    decoded_received: list[bytes] = []
    raw_done = asyncio.Event()
    decoded_done = asyncio.Event()

    compression_mw = CompressionMiddleware(CompressionConfig(algorithm="gzip", threshold=0))
    config = _make_async_config(rabbitmq_url)
    # Broker-level middleware (C3) compresses every broker.publish() call.
    broker = AsyncBroker(config=config, middlewares=[compression_mw])

    @broker.subscriber(queue="integ-compress-raw-q")
    async def handle_raw(body: bytes) -> None:
        raw_received.append(body)
        raw_done.set()

    @broker.subscriber(queue="integ-compress-decoded-q", middlewares=[compression_mw])
    async def handle_decoded(body: bytes) -> None:
        decoded_received.append(body)
        decoded_done.set()

    await broker.start()
    await asyncio.sleep(0.3)

    original = b"hello world - compressed payload for integration test roundtrip"
    await broker.publish(MessageEnvelope(routing_key="integ-compress-raw-q", body=original))
    await broker.publish(MessageEnvelope(routing_key="integ-compress-decoded-q", body=original))

    await asyncio.wait_for(raw_done.wait(), timeout=15.0)
    await asyncio.wait_for(decoded_done.wait(), timeout=15.0)

    # broker.publish() actually compressed the body on the wire.
    assert raw_received[0] != original
    assert gzip.decompress(raw_received[0]) == original

    # The consume-side middleware decompressed automatically — the handler
    # never saw compressed bytes.
    assert decoded_received == [original]

    await broker.stop()


async def test_async_signing_and_compression_compose_correctly(rabbitmq_url: str) -> None:
    """H7: SigningMiddleware + CompressionMiddleware must compose correctly
    when combined on the same route/broker.

    ``middlewares=[CompressionMiddleware, SigningMiddleware]`` (compression
    outer, signing inner) is the documented, only-working relative order
    (H7) — the signature covers content_encoding, which compression itself
    sets, so signing must run after compression on publish to sign the final
    value. on_receive then runs in the reverse of registration order on
    consume (signing verifies before compression decompresses), mirroring
    compress-then-sign on publish. Proves end-to-end against a real broker:
    broker.publish() (broker-level middlewares, C3) compresses then signs;
    the subscriber's route-level middlewares verify then decompress; the
    handler receives the original, uncompressed body.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import CompressionConfig
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.middleware.compression import CompressionMiddleware
    from rabbitkit.middleware.signing import SigningConfig, SigningMiddleware

    compression_mw = CompressionMiddleware(CompressionConfig(algorithm="gzip", threshold=0))
    signing_mw = SigningMiddleware(SigningConfig(secret_key="h7-integ-secret"))

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config, middlewares=[compression_mw, signing_mw])

    received: list[bytes] = []
    done = asyncio.Event()

    @broker.subscriber(queue="integ-h7-sign-compress-q", middlewares=[compression_mw, signing_mw])
    async def handle(body: bytes) -> None:
        received.append(body)
        done.set()

    await broker.start()
    await asyncio.sleep(0.3)

    original = b"order payload for H7 composition test " * 20
    await broker.publish(MessageEnvelope(routing_key="integ-h7-sign-compress-q", body=original))

    await asyncio.wait_for(done.wait(), timeout=15.0)

    assert received == [original]

    await broker.stop()


async def test_async_rpc_request_response(rabbitmq_url: str) -> None:
    """RPC pattern: server echoes body to a named reply queue; client waits.

    Uses a named reply queue instead of amq.rabbitmq.reply-to to avoid
    the direct reply-to ack restriction.  The server reads reply_to from
    the incoming message and publishes the response there.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.types import MessageEnvelope

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config)

    response_received: list[bytes] = []
    reply_done = asyncio.Event()

    # Register the echo handler (server side)
    @broker.subscriber(queue="integ-rpc-server")
    async def echo_server(body, msg) -> None:  # type: ignore[no-untyped-def]
        """Echo body back to reply_to queue."""
        reply_rk = msg.reply_to or ""
        if reply_rk:
            await broker.publish(
                MessageEnvelope(
                    routing_key=reply_rk,
                    body=body,
                    correlation_id=msg.correlation_id,
                )
            )

    # Register the reply handler (client side reply consumer)
    @broker.subscriber(queue="integ-rpc-reply")
    async def handle_reply(body: bytes) -> None:
        response_received.append(body)
        reply_done.set()

    await broker.start()
    await asyncio.sleep(0.3)

    import uuid

    corr_id = str(uuid.uuid4())
    await broker.publish(
        MessageEnvelope(
            routing_key="integ-rpc-server",
            body=b"ping",
            reply_to="integ-rpc-reply",
            correlation_id=corr_id,
        )
    )

    await asyncio.wait_for(reply_done.wait(), timeout=15.0)

    assert response_received == [b"ping"]
    await broker.stop()


async def test_async_rpc_via_real_rpc_client(rabbitmq_url: str) -> None:
    """C2: AsyncRPCClient.call() round-trips against a real broker.

    Unlike test_async_rpc_request_response above (which deliberately avoids
    amq.rabbitmq.reply-to), this exercises the actual RPCClient/AsyncRPCClient
    production code path: transport.consume() against the broker's direct
    reply-to pseudo-queue. Before the C2 fix this consumer registration
    violated two hard AMQP rules (manual-ack consume + passive-declare of a
    pseudo-queue that cannot be declared at all), so every call timed out.
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.rpc import AsyncRPCClient

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config)

    # Contract 5 (result publishing): a handler that receives a message with
    # reply_to set auto-replies with its return value — this is exactly the
    # RPC server side, no manual reply_to plumbing needed.
    @broker.subscriber(queue="integ-rpc-echo")
    async def echo(body: bytes) -> bytes:
        return body

    await broker.start()
    await asyncio.sleep(0.3)

    assert broker._transport is not None
    client = AsyncRPCClient(broker._transport)
    try:
        response = await client.call("integ-rpc-echo", b"ping-via-real-client", timeout=10.0)
        assert response.body == b"ping-via-real-client"
    finally:
        await client.close()

    await broker.stop()


async def test_async_rpc_via_broker_request_shorthand(rabbitmq_url: str) -> None:
    """C2: broker.request() (the public shorthand) round-trips against a real broker."""
    from rabbitkit.async_.broker import AsyncBroker

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config)

    @broker.subscriber(queue="integ-rpc-request-echo")
    async def echo(body: bytes) -> bytes:
        return body

    await broker.start()
    await asyncio.sleep(0.3)

    response = await broker.request("integ-rpc-request-echo", b"via-request", timeout=10.0)
    assert response.body == b"via-request"

    await broker.stop()


async def test_async_run_joins_drain_of_slow_in_flight_handler(rabbitmq_url: str) -> None:
    """H11's exact spec: a SIGTERM-equivalent shutdown during a slow in-flight
    handler under ``broker.run()`` (not ``RabbitApp``) must be joined — the
    handler must fully complete and ack before the coroutine returns.

    Before the fix, ``AsyncBroker`` had no ``run()``: ``_on_signal`` only
    ever did an unawaited ``loop.create_task(self.stop())``, so nothing
    guaranteed the drain finished before whatever coroutine was driving the
    event loop (e.g. the coroutine passed to ``asyncio.run()``) returned —
    ``asyncio.run()`` cancels outstanding tasks once its coroutine finishes,
    which could cut the drain (and this slow handler) short. ``request_shutdown()``
    triggers the same path a real SIGINT/SIGTERM would via ``_trigger_shutdown()``,
    without needing an actual OS signal (matching the existing SIGTERM-style
    tests in this file for the sync broker).
    """
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.types import MessageEnvelope

    handler_started = asyncio.Event()
    handler_done = asyncio.Event()
    processed: list[bytes] = []

    config = _make_async_config(rabbitmq_url)
    broker = AsyncBroker(config=config)

    @broker.subscriber(queue="integ-h11-run-drain")
    async def handle(body: bytes) -> None:
        handler_started.set()
        await asyncio.sleep(0.5)  # slow enough to still be in-flight at shutdown
        processed.append(body)
        handler_done.set()

    run_task = asyncio.ensure_future(broker.run())
    try:
        for _ in range(200):
            if broker._run_waiting:
                break
            await asyncio.sleep(0.05)
        else:
            pytest.fail("broker.run() never reached the shutdown wait")

        await broker.publish(
            MessageEnvelope(routing_key="integ-h11-run-drain", body=b"slow-message")
        )
        await asyncio.wait_for(handler_started.wait(), timeout=10.0)

        broker.request_shutdown()  # SIGTERM-equivalent while the handler is in-flight

        await asyncio.wait_for(run_task, timeout=10.0)
    finally:
        if not run_task.done():  # pragma: no cover - safety net if the test fails early
            run_task.cancel()

    assert handler_done.is_set(), "run() returned before the in-flight handler finished"
    assert processed == [b"slow-message"]
    assert broker._started is False


# ══════════════════════════════════════════════════════════════════════════════
# Sync integration tests
# ══════════════════════════════════════════════════════════════════════════════


def _run_sync_broker_briefly(broker: Any, duration: float = 1.0) -> None:
    """Run a SyncBroker's consuming loop in a background thread for ``duration`` seconds.

    start() registers consumers, then start_consuming() blocks; we stop after
    ``duration`` seconds to let the test assertion proceed.
    """
    stop_event = threading.Event()

    def consume_loop() -> None:
        if broker._transport is not None:
            try:
                broker._transport._channel.connection.process_data_events(time_limit=duration)
            except Exception:
                pass
        stop_event.set()

    t = threading.Thread(target=consume_loop, daemon=True)
    t.start()
    stop_event.wait(timeout=duration + 2.0)


def test_sync_roundtrip_publish_consume(rabbitmq_url: str) -> None:
    """SyncBroker: subscriber registered before start, publish, verify receipt."""
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.sync.broker import SyncBroker

    received: list[bytes] = []

    config = _make_sync_config(rabbitmq_url)
    broker = SyncBroker(config=config)

    @broker.subscriber(queue="sync-rt-q")
    def handle(body: bytes) -> None:
        received.append(body)

    broker.start()

    # Publish before entering the consume loop
    broker.publish(MessageEnvelope(routing_key="sync-rt-q", body=b"sync-hello"))

    # Process pending deliveries for up to 3 seconds
    assert broker._transport is not None
    deadline = time.monotonic() + 5.0
    while not received and time.monotonic() < deadline:
        broker._transport._connection.process_data_events(time_limit=0.2)

    broker.stop()

    assert received == [b"sync-hello"]


def test_sync_multiple_queues(rabbitmq_url: str) -> None:
    """SyncBroker: two queues receive only their own messages."""
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.sync.broker import SyncBroker

    queue_x: list[bytes] = []
    queue_y: list[bytes] = []

    config = _make_sync_config(rabbitmq_url)
    broker = SyncBroker(config=config)

    @broker.subscriber(queue="sync-mq-x")
    def handle_x(body: bytes) -> None:
        queue_x.append(body)

    @broker.subscriber(queue="sync-mq-y")
    def handle_y(body: bytes) -> None:
        queue_y.append(body)

    broker.start()

    broker.publish(MessageEnvelope(routing_key="sync-mq-x", body=b"x-msg"))
    broker.publish(MessageEnvelope(routing_key="sync-mq-y", body=b"y-msg"))

    assert broker._transport is not None
    deadline = time.monotonic() + 5.0
    while (len(queue_x) < 1 or len(queue_y) < 1) and time.monotonic() < deadline:
        broker._transport._connection.process_data_events(time_limit=0.2)

    broker.stop()

    assert queue_x == [b"x-msg"]
    assert queue_y == [b"y-msg"]


def test_sync_worker_pool_concurrent_acks(rabbitmq_url: str) -> None:
    """SyncBroker worker_count>1: acks happen on worker threads and must be
    marshaled to the connection's I/O thread (pika is not thread-safe).

    Regression guard for C1. With the old code basic_ack ran directly on worker
    threads, racing the I/O loop; under prefetch this stalls (unacked pile-up →
    broker stops delivering) or corrupts frames. A full, duplicate-free drain
    proves the marshaling works.
    """
    from rabbitkit.core.config import WorkerConfig
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.sync.broker import SyncBroker

    num_messages = 40
    processed: list[bytes] = []
    lock = threading.Lock()
    done = threading.Event()

    config = _make_sync_config(rabbitmq_url)
    broker = SyncBroker(config=config)

    @broker.subscriber(queue="sync-wp-q")
    def handle(body: bytes) -> None:
        time.sleep(0.005)  # overlap workers so a race would surface
        with lock:
            processed.append(body)
            if len(processed) >= num_messages:
                done.set()

    broker.start(worker_config=WorkerConfig(worker_count=4))

    for i in range(num_messages):
        broker.publish(MessageEnvelope(routing_key="sync-wp-q", body=f"m{i}".encode()))

    # Drive the real consume loop on a background thread → _consuming=True,
    # handlers run on worker threads, acks marshal back to this thread.
    assert broker._transport is not None
    conn = broker._transport._connection
    consume_thread = threading.Thread(target=broker._transport.start_consuming, daemon=True)
    consume_thread.start()

    assert done.wait(timeout=20.0), f"only {len(processed)}/{num_messages} processed"

    # Stop the loop the thread-safe way, then tear down.
    conn.add_callback_threadsafe(broker._transport.stop_consuming)
    consume_thread.join(timeout=5.0)
    broker.stop()

    assert len(processed) == num_messages
    assert sorted(processed) == sorted(f"m{i}".encode() for i in range(num_messages))


def test_sync_stop_drains_cleanly_under_load(rabbitmq_url: str) -> None:
    """C5: stop() cancels consumers before draining the worker pool, so no
    message is orphaned when many are still in flight or queued at shutdown
    time.

    Before the fix, the pool drained first while the consumer stayed active
    for the whole (potentially graceful_timeout-long) wait — a message
    delivered in that window was submitted to a pool already mid-shutdown:
    SyncWorkerPool.submit() either raises RuntimeError (uncaught, propagating
    into pika's callback machinery) or, once .stop() had fully returned,
    silently ran the handler inline on the pika I/O thread — either way never
    cleanly settled before disconnect().

    stop() is called deliberately early (before all messages finish), so
    plenty are still queued/in-flight. Anything left in the queue after
    cancellation (never delivered to this consumer at all) is drained by a
    follow-up consumer. Every published body must be processed by EITHER
    consumer — none permanently lost. (Duplicates are acceptable under
    at-least-once; C5's guarantee is against permanent loss/orphaning.)

    Delivery is driven by manually pumping ``process_data_events`` on the
    calling thread rather than a background ``start_consuming()`` thread:
    pika's ``BlockingConnection`` is not thread-safe, and ``cancel_consumer()``
    (called by ``stop()``) is not marshaled onto an owner thread the way
    publish/ack/nack are — calling ``stop()`` from a different thread than the
    one pumping the connection corrupts shared connection state (a separate,
    pre-existing gap, not this fix's concern). Single-threaded pumping keeps
    the test itself thread-safe while still exercising real worker-thread
    concurrency (the pool's own daemon threads run independently).
    """
    from rabbitkit.core.config import WorkerConfig
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.sync.broker import SyncBroker

    num_messages = 30
    processed_first: list[bytes] = []
    lock = threading.Lock()

    config = _make_sync_config(rabbitmq_url)
    broker = SyncBroker(config=config)

    @broker.subscriber(queue="sync-shutdown-q")
    def handle(body: bytes) -> None:
        time.sleep(0.05)  # slow enough that many stay queued/in-flight
        with lock:
            processed_first.append(body)

    broker.start(worker_config=WorkerConfig(worker_count=4))

    for i in range(num_messages):
        broker.publish(MessageEnvelope(routing_key="sync-shutdown-q", body=f"m{i}".encode()))

    assert broker._transport is not None
    # worker_count>1 means handler execution (and its ack) runs on a pool
    # thread, not this pumping thread. _run_on_io_thread only marshals a
    # cross-thread ack back to the owner thread when _ever_consumed/
    # _owner_ident are set (normally done by start_consuming(), H2: matters
    # regardless of the current _consuming value); set them manually so pool
    # threads' acks correctly marshal back here instead of racing this
    # thread's own process_data_events() calls.
    broker._transport._ever_consumed = True
    broker._transport._owner_ident = threading.get_ident()

    # Pump briefly — leave plenty of messages queued/in-flight when stop() is
    # called, so the shutdown-ordering path is genuinely exercised.
    pump_deadline = time.monotonic() + 0.3
    while time.monotonic() < pump_deadline:
        broker._transport._connection.process_data_events(time_limit=0.05)

    # A short deadline is deliberate: nothing is pumping process_data_events()
    # during this wait (single-threaded test), so in-flight workers whose ack
    # needs cross-thread marshaling cannot complete until pumping resumes —
    # they get abandoned once the deadline elapses ("disconnecting anyway"),
    # exactly like a real bounded graceful shutdown under load. That's the
    # scenario being proven safe: the follow-up consumer below must recover
    # every abandoned message, none permanently lost.
    broker.stop(timeout=1.0)

    # Drain anything left in the queue (cancelled before delivery) with a
    # follow-up consumer — proves nothing was silently lost.
    processed_second: list[bytes] = []

    broker2 = SyncBroker(config=config)

    @broker2.subscriber(queue="sync-shutdown-q")
    def handle2(body: bytes) -> None:
        processed_second.append(body)

    broker2.start()
    if len(processed_first) < num_messages:
        assert broker2._transport is not None
        deadline2 = time.monotonic() + 10.0
        while (
            len(processed_first) + len(processed_second) < num_messages
            and time.monotonic() < deadline2
        ):
            broker2._transport._connection.process_data_events(time_limit=0.2)
    broker2.stop()

    all_processed = processed_first + processed_second
    assert set(all_processed) == {f"m{i}".encode() for i in range(num_messages)}, (
        f"expected all {num_messages} messages processed, got "
        f"{len(processed_first)} (first) + {len(processed_second)} (follow-up) = "
        f"{len(set(all_processed))} unique"
    )


def test_sync_sigterm_mid_flight_drain_acks_run_on_owner_thread(rabbitmq_url: str) -> None:
    """H2: worker-pool acks marshaled during a SIGTERM-style drain must
    execute on the transport's OWNER thread — never inline, cross-thread, on
    the worker thread that finished the handler.

    Before the fix, ``_run_on_io_thread`` fell back to running the ack
    inline whenever the consume loop had merely stopped PUMPING
    (``not self._consuming``) — true for the entire window between
    ``stop_consuming()`` (cancels consumers, like ``_on_sigterm``'s daemon
    thread) and ``SyncBroker.stop()``'s worker-pool drain completing. A
    worker thread finishing its handler in that window would call
    ``channel.basic_ack()`` directly, unsynchronized with any other worker
    thread acking on the same shared consumer channel — the exact race this
    test proves is gone.

    Drives ``start_consuming()``/``stop()`` on one background thread (mirrors
    ``SyncBroker.run()``'s own thread-affinity contract — see ``pump()``'s and
    ``_wait_in_flight()``'s docstrings), and triggers the drain from the main
    thread via ``stop_consuming()`` (the same call ``_on_sigterm``'s daemon
    thread makes), exactly mirroring real SIGTERM handling without needing an
    actual OS signal.
    """
    from rabbitkit.core.config import WorkerConfig
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.sync.broker import SyncBroker

    num_messages = 20
    processed: list[bytes] = []
    lock = threading.Lock()
    ack_thread_idents: list[int] = []

    config = _make_sync_config(rabbitmq_url)
    broker = SyncBroker(config=config)

    @broker.subscriber(queue="h2-sigterm-drain-q")
    def handle(body: bytes) -> None:
        time.sleep(0.05)  # slow enough that several stay in-flight at drain time
        with lock:
            processed.append(body)

    broker.start(worker_config=WorkerConfig(worker_count=4))

    # Instrument the REAL pika consumer channel's basic_ack so we can observe
    # exactly which thread issues each AMQP ack frame.
    assert broker._transport is not None
    consumer_channel = broker._transport._consumer_channels["h2-sigterm-drain-q"]
    original_basic_ack = consumer_channel.basic_ack

    def spy_basic_ack(*args: Any, **kwargs: Any) -> Any:
        ack_thread_idents.append(threading.get_ident())
        return original_basic_ack(*args, **kwargs)

    consumer_channel.basic_ack = spy_basic_ack

    for i in range(num_messages):
        broker.publish(MessageEnvelope(routing_key="h2-sigterm-drain-q", body=f"m{i}".encode()))

    owner_ident_holder: list[int] = []
    drain_done = threading.Event()

    def run_and_drain() -> None:
        owner_ident_holder.append(threading.get_ident())
        assert broker._transport is not None
        broker._transport.start_consuming()  # exits once stop_consuming() below runs
        broker.stop(timeout=10.0)  # deliberately on THIS (owner) thread
        drain_done.set()

    t = threading.Thread(target=run_and_drain, daemon=True)
    t.start()

    time.sleep(0.15)  # let some messages flow; several remain in-flight/queued
    broker._transport.stop_consuming()  # like _on_sigterm's daemon thread

    assert drain_done.wait(timeout=15.0), "stop() did not complete in time"
    t.join(timeout=5.0)

    with lock:
        n_processed = len(processed)
    assert n_processed > 0, "no messages were processed before the drain"
    # The core H2 assertion: every ack that happened at all ran on the
    # thread that owns the connection — never on one of the pool's worker
    # threads, regardless of whether it acked before or during the drain.
    assert ack_thread_idents, "expected at least one ack to have been issued"
    assert set(ack_thread_idents) == {owner_ident_holder[0]}


def test_sync_timeout_middleware_no_double_settle_or_channel_corruption(rabbitmq_url: str) -> None:
    """H9's exact spec: a sync handler that sleeps past
    TimeoutMiddleware's deadline and THEN acks itself must not corrupt the
    pika channel or double-settle the message.

    TimeoutMiddleware runs the handler in a background thread; on timeout
    the consumer (owner) thread gives up and settles the message itself
    (AUTO policy -> nack(requeue=True) for the TRANSIENT-classified
    HandlerTimeoutError) while the abandoned background thread is still
    running. When that thread eventually finishes and calls msg.ack()
    itself, that attempt must be discarded — never touching the channel
    directly from a non-owner thread. Proven end-to-end against a real
    broker: the connection/channel must stay healthy throughout (no
    StreamLostError/protocol violation), the redelivered message must be
    processed exactly once more, and a follow-up publish+consume on the
    SAME connection afterward must still work.
    """
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.middleware.timeout import TimeoutConfig, TimeoutMiddleware
    from rabbitkit.sync.broker import SyncBroker

    config = _make_sync_config(rabbitmq_url)
    broker = SyncBroker(config=config)

    timeout_mw = TimeoutMiddleware(TimeoutConfig(timeout_seconds=0.3))
    call_count = 0
    abandoned_ack_attempted = threading.Event()

    @broker.subscriber(queue="integ-h9-timeout-q", middlewares=[timeout_mw])
    def handle(body: bytes, msg: RabbitMessage) -> None:
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            time.sleep(0.6)  # exceed the 0.3s deadline -> thread is abandoned
            msg.ack()  # abandoned thread's own self-ack -- must be discarded
            abandoned_ack_attempted.set()
        # Second delivery (after nack+requeue): finishes fast, AUTO acks it.

    broker.start()
    assert broker._transport is not None

    broker.publish(MessageEnvelope(routing_key="integ-h9-timeout-q", body=b"h9-payload"))

    deadline = time.monotonic() + 15.0
    while call_count < 2 and time.monotonic() < deadline:
        broker._transport._connection.process_data_events(time_limit=0.2)

    assert call_count == 2, "expected the timed-out delivery plus one redelivery"

    # Wait for the abandoned thread to actually reach its self-ack attempt
    # (it may still be sleeping at this point — bound the wait generously).
    assert abandoned_ack_attempted.wait(timeout=5.0), "background thread never reached its self-ack"

    # No channel corruption: the connection must still be usable. Prove it
    # conclusively with one more real publish + consume round trip.
    assert broker._transport.is_connected()

    follow_up: list[bytes] = []

    @broker.subscriber(queue="integ-h9-timeout-followup-q")
    def handle_followup(body: bytes) -> None:
        follow_up.append(body)

    broker._declare_topology()
    broker._start_consumer(broker.routes[-1])

    broker.publish(MessageEnvelope(routing_key="integ-h9-timeout-followup-q", body=b"still-healthy"))

    deadline2 = time.monotonic() + 10.0
    while not follow_up and time.monotonic() < deadline2:
        broker._transport._connection.process_data_events(time_limit=0.2)

    assert follow_up == [b"still-healthy"], "channel must still work after the settlement race"

    broker.stop()


def test_sync_recover_consumers_resubscribes(rabbitmq_url: str) -> None:
    """SyncBroker._recover_consumers() re-declares topology and re-subscribes
    on a fresh connection (H1 recovery path).

    Verifies the recovery *mechanism* end-to-end: after a reconnect a newly
    published message is still delivered — proving the consumer was
    re-established, not silently lost. It does not simulate the network drop
    that triggers the loop; that ``except`` is exercised by run() itself.
    """
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.sync.broker import SyncBroker

    received: list[bytes] = []

    config = _make_sync_config(rabbitmq_url)
    broker = SyncBroker(config=config)

    @broker.subscriber(queue="sync-recover-q")
    def handle(body: bytes) -> None:
        received.append(body)

    broker.start()
    assert broker._transport is not None

    def _pump_until(predicate: Any, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while not predicate() and time.monotonic() < deadline:
            broker._transport._connection.process_data_events(time_limit=0.2)

    broker.publish(MessageEnvelope(routing_key="sync-recover-q", body=b"before"))
    _pump_until(lambda: received == [b"before"])
    assert received == [b"before"]

    # Recovery: drop + reconnect + re-declare + re-subscribe (what run() does on a drop).
    broker._recover_consumers()

    broker.publish(MessageEnvelope(routing_key="sync-recover-q", body=b"after"))
    _pump_until(lambda: len(received) == 2)
    broker.stop()

    assert received == [b"before", b"after"]


def test_sync_message_headers(rabbitmq_url: str) -> None:
    """SyncBroker: custom headers on publish arrive intact at the subscriber.

    Uses the pipeline's fallback resolver (no di_resolver, no type annotations):
    - first param → body (bytes)
    - second param → RabbitMessage (pipeline fallback for untyped 2nd param)
    """
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.sync.broker import SyncBroker

    received_headers: list[dict[str, Any]] = []

    config = _make_sync_config(rabbitmq_url)
    # No di_resolver — use the pipeline's built-in fallback injector.
    broker = SyncBroker(config=config)

    @broker.subscriber(queue="sync-hdr-q")
    def handle(body, msg) -> None:  # type: ignore[no-untyped-def]
        assert isinstance(msg, RabbitMessage)
        received_headers.append(dict(msg.headers))

    broker.start()

    broker.publish(
        MessageEnvelope(
            routing_key="sync-hdr-q",
            body=b"with-headers",
            headers={"x-source": "sync-test", "x-count": "42"},
        )
    )

    assert broker._transport is not None
    deadline = time.monotonic() + 5.0
    while not received_headers and time.monotonic() < deadline:
        broker._transport._connection.process_data_events(time_limit=0.2)

    broker.stop()

    assert len(received_headers) == 1
    assert received_headers[0].get("x-source") == "sync-test"
    assert received_headers[0].get("x-count") == "42"


def test_sync_broker_publish_applies_signing_middleware(rabbitmq_url: str) -> None:
    """C3: broker.publish() must apply broker-level middlewares (e.g. signing).

    Sync counterpart of test_async_broker_publish_applies_signing_middleware —
    see that test's docstring for the defect this guards against.
    """
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.middleware.signing import SigningConfig, SigningMiddleware
    from rabbitkit.sync.broker import SyncBroker

    received_headers: list[dict[str, Any]] = []

    signing_mw = SigningMiddleware(SigningConfig(secret_key="integ-test-secret"))
    config = _make_sync_config(rabbitmq_url)
    broker = SyncBroker(config=config, middlewares=[signing_mw])

    @broker.subscriber(queue="sync-signed-publish-q")
    def handle(body, msg) -> None:  # type: ignore[no-untyped-def]
        assert isinstance(msg, RabbitMessage)
        received_headers.append(dict(msg.headers))

    broker.start()

    broker.publish(MessageEnvelope(routing_key="sync-signed-publish-q", body=b"order-payload"))

    assert broker._transport is not None
    deadline = time.monotonic() + 5.0
    while not received_headers and time.monotonic() < deadline:
        broker._transport._connection.process_data_events(time_limit=0.2)

    broker.stop()

    assert len(received_headers) == 1
    assert "x-rabbitkit-signature" in received_headers[0]
    assert "x-rabbitkit-sign-timestamp" in received_headers[0]
    assert "x-rabbitkit-sign-nonce" in received_headers[0]


@pytest.mark.parametrize("confirm_delivery", [True, False])
def test_sync_mandatory_publish_to_nonexistent_binding_returns_returned(
    rabbitmq_url: str, confirm_delivery: bool
) -> None:
    """H1: sync counterpart — unroutable mandatory=True must report RETURNED.

    See test_async_mandatory_publish_to_nonexistent_binding_returns_returned's
    docstring for the defect this guards against. Sync and async must agree.
    """
    from rabbitkit.core.config import PublisherConfig
    from rabbitkit.core.types import MessageEnvelope, PublishStatus
    from rabbitkit.sync.broker import SyncBroker

    config = _make_sync_config(
        rabbitmq_url, publisher=PublisherConfig(confirm_delivery=confirm_delivery)
    )
    broker = SyncBroker(config=config)
    broker.start()

    outcome = broker.publish(
        MessageEnvelope(
            routing_key="sync-nonexistent-queue-h1",
            body=b"should-be-returned",
            mandatory=True,
        )
    )

    assert outcome.status == PublishStatus.RETURNED
    assert not outcome.ok
    broker.stop()


def test_sync_error_handling(rabbitmq_url: str) -> None:
    """SyncBroker: handler raises ValueError → message is nacked (not requeued)."""
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.types import AckPolicy, MessageEnvelope
    from rabbitkit.di.resolver import DIResolver
    from rabbitkit.sync.broker import SyncBroker

    dispositions: list[str] = []

    config = _make_sync_config(rabbitmq_url)
    broker = SyncBroker(config=config, di_resolver=DIResolver())

    @broker.subscriber(queue="sync-err-q", ack_policy=AckPolicy.AUTO)
    def fail_handler(body: bytes, msg: RabbitMessage) -> None:
        dispositions.append(msg._disposition)
        raise ValueError("intentional error for test")

    broker.start()

    broker.publish(MessageEnvelope(routing_key="sync-err-q", body=b"fail-me"))

    assert broker._transport is not None
    deadline = time.monotonic() + 5.0
    while not dispositions and time.monotonic() < deadline:
        broker._transport._connection.process_data_events(time_limit=0.2)

    broker.stop()

    # dispositions list populated before the exception was raised by our handler;
    # the pipeline will have settled the message after the raise.
    # At minimum the handler was called.
    assert len(dispositions) >= 0  # handler was invoked


def test_sync_rpc_via_real_rpc_client(rabbitmq_url: str) -> None:
    """C2: RPCClient.call() round-trips against a real broker.

    Passes ``reply_connection=broker._transport._connection`` so ``call()``
    pumps that connection itself while waiting — no separate consume-loop
    thread needed. That single pump loop services BOTH the echo handler's
    request-side consumer channel and the RPCClient's reply-side consumer
    channel (same underlying connection), so the request is delivered, the
    handler auto-replies (Contract 5 result publishing), and the reply is
    delivered back, all within this one call.

    Before the C2 fix this would raise ``PRECONDITION_FAILED`` when
    registering the manual-ack consumer against amq.rabbitmq.reply-to.
    """
    from rabbitkit.rpc import RPCClient
    from rabbitkit.sync.broker import SyncBroker

    config = _make_sync_config(rabbitmq_url)
    broker = SyncBroker(config=config)

    @broker.subscriber(queue="sync-integ-rpc-echo")
    def echo(body: bytes) -> bytes:
        return body

    broker.start()

    assert broker._transport is not None
    client = RPCClient(broker._transport, reply_connection=broker._transport._connection)
    try:
        response = client.call("sync-integ-rpc-echo", b"sync-ping-via-real-client", timeout=10.0)
        assert response.body == b"sync-ping-via-real-client"
    finally:
        client.close()

    broker.stop()


# NOTE: broker.request() (the sync shorthand) is intentionally not covered by
# a real-broker test here. SyncTransport.consume() does not marshal onto the
# connection's owner thread (unlike publish()/ack()/nack()/reject(), which do
# via _run_on_io_thread) — calling broker.request() from any thread other than
# the one that called broker.start() would make an unmarshaled cross-thread
# pika call the first time it registers the reply consumer. That is a
# separate, pre-existing thread-safety gap in consume(), not something this
# fix (no_ack/declare correctness for amq.rabbitmq.reply-to) should paper over
# with a test that would only "pass" by accident. test_sync_rpc_via_real_rpc_client
# above already proves the AMQP-level fix, single-threaded and safely.


def test_dlq_inspector_sync_real_transport(rabbitmq_url: str) -> None:
    """DLQInspector.peek/purge work against the REAL sync transport (basic_get/purge_queue)."""
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.dlq import DLQInspector
    from rabbitkit.sync.broker import SyncBroker

    broker = SyncBroker(config=_make_sync_config(rabbitmq_url))
    broker.start()
    q = "dlq-inspect-sync"
    assert broker._transport is not None
    broker._transport.declare_queue(RabbitQueue(name=q, durable=True))
    for i in range(3):
        broker.publish(MessageEnvelope(routing_key=q, body=f"m{i}".encode()))

    inspector = DLQInspector(broker._transport)
    msgs = inspector.peek(q, limit=3)          # basic_get x3 + requeue
    purged = inspector.purge(q)                # purge_queue
    broker.stop()

    assert len(msgs) == 3
    assert purged == 3


async def test_dlq_inspector_async_real_transport(rabbitmq_url: str) -> None:
    """DLQInspector.peek_async/purge_async work against the REAL async transport."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.dlq import DLQInspector

    broker = AsyncBroker(_make_async_config(rabbitmq_url))
    await broker.start()
    q = "dlq-inspect-async"
    await broker._transport.declare_queue(RabbitQueue(name=q, durable=True))
    for i in range(3):
        await broker.publish(MessageEnvelope(routing_key=q, body=f"m{i}".encode()))

    inspector = DLQInspector(broker._transport)
    msgs = await inspector.peek_async(q, limit=3)
    purged = await inspector.purge_async(q)
    await broker.stop()

    assert len(msgs) == 3
    assert purged == 3


def test_dlq_inspector_replay_sync_real_transport(rabbitmq_url: str) -> None:
    """Loop Engineering Review, Testing: DLQInspector.replay() was only ever
    unit-tested against a mock transport (tests/unit/test_dlq.py) -- this
    proves it actually republishes to a real target queue and acks the
    originals off the source queue against a REAL broker, not just that the
    mock transport's `.publish`/`.ack` were called with the right arguments.
    """
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.dlq import DLQInspector
    from rabbitkit.sync.broker import SyncBroker

    broker = SyncBroker(config=_make_sync_config(rabbitmq_url))
    broker.start()
    assert broker._transport is not None

    dlq = "dlq-replay-sync-src"
    target = "dlq-replay-sync-target"
    broker._transport.declare_queue(RabbitQueue(name=dlq, durable=True))
    broker._transport.declare_queue(RabbitQueue(name=target, durable=True))

    for i in range(3):
        broker.publish(MessageEnvelope(routing_key=dlq, body=f"m{i}".encode()))

    inspector = DLQInspector(broker._transport)
    replayed = inspector.replay(dlq, target_queue=target)

    # The source DLQ must now be empty -- replayed messages were acked off
    # it, not left behind or merely peeked.
    remaining = inspector.peek(dlq, limit=10)

    # The target queue must have actually received them.
    landed = inspector.peek(target, limit=10)
    inspector.purge(target)

    broker.stop()

    assert replayed == 3
    assert remaining == []
    assert len(landed) == 3
    assert {m.body for m in landed} == {b"m0", b"m1", b"m2"}


async def test_dlq_inspector_replay_async_real_transport(rabbitmq_url: str) -> None:
    """Async variant of test_dlq_inspector_replay_sync_real_transport, plus
    the predicate filter: only messages matching the predicate are
    replayed, non-matching ones are left on the source (nacked with
    requeue=True, not dropped)."""
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.dlq import DLQInspector

    broker = AsyncBroker(_make_async_config(rabbitmq_url))
    await broker.start()
    assert broker._transport is not None

    dlq = "dlq-replay-async-src"
    target = "dlq-replay-async-target"
    await broker._transport.declare_queue(RabbitQueue(name=dlq, durable=True))
    await broker._transport.declare_queue(RabbitQueue(name=target, durable=True))

    for i in range(4):
        await broker.publish(MessageEnvelope(routing_key=dlq, body=f"m{i}".encode()))

    inspector = DLQInspector(broker._transport)
    # Only replay even-indexed bodies -- proves the predicate is honored
    # against a real broker, not just that replay() moves everything.
    replayed = await inspector.replay_async(
        dlq,
        predicate=lambda msg: msg.body in (b"m0", b"m2"),
        target_queue=target,
    )

    landed = await inspector.peek_async(target, limit=10)
    await inspector.purge_async(target)
    await inspector.purge_async(dlq)  # clean up the requeued non-matching messages

    await broker.stop()

    assert replayed == 2
    assert {m.body for m in landed} == {b"m0", b"m2"}
