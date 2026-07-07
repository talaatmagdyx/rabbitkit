"""End-to-end two-stage pipeline against a real RabbitMQ — Twitter-DM shaped.

Scenario (same pipeline implemented twice, sync and async, verified for
identical semantics):

    mock DM producer ──> dm.events ──> relay (enrich) ──> dm.enriched ──> sink

- The producer publishes N mock Twitter DM events (JSON) to the ingest queue.
- The relay stage consumes them, applies a deterministic enrichment
  (normalize text, extract mentions/hashtags, classify sentiment), and
  forwards the result to the enriched queue via the ``@publisher`` result
  path — which means the SOURCE message is acked only after the enriched
  publish is broker-confirmed (the message-safety invariant this pipeline
  exists to prove at volume).
- The sink consumes the enriched queue and the test verifies: zero loss
  (every id arrives), zero duplication (clean run), and byte-for-byte
  correct enrichment against the same pure function both relays share —
  which is also the sync/async parity proof.

Volume: ``RABBITKIT_PIPELINE_EVENTS`` (default 5_000 — a few seconds in
CI). To genuinely run millions locally::

    RABBITKIT_PIPELINE_EVENTS=1000000 .venv/bin/pytest \
        tests/integration/test_pipeline_twitter_dm.py -m integration -q --timeout=3600

Run with::

    pytest tests/integration/test_pipeline_twitter_dm.py -m integration -v
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import threading
import time
from typing import Any

import pytest

try:
    from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

    _TESTCONTAINERS_AVAILABLE = True
except ImportError:
    _TESTCONTAINERS_AVAILABLE = False

pytestmark = pytest.mark.integration

logger = logging.getLogger(__name__)

N_EVENTS = int(os.environ.get("RABBITKIT_PIPELINE_EVENTS", "5000"))
# Generous ceiling that scales with volume: sync relay publishes are
# confirm-gated one at a time (~1k msg/s floor), async is much faster.
PIPELINE_TIMEOUT = max(60.0, N_EVENTS / 500)


def _skip_no_docker() -> None:
    if not _TESTCONTAINERS_AVAILABLE:
        pytest.skip("testcontainers not installed")
    try:
        import docker  # type: ignore[import-untyped]

        docker.from_env().ping()
    except Exception:
        pytest.skip("Docker daemon not reachable")


@pytest.fixture(scope="module")
def rabbitmq_url() -> str:  # type: ignore[return]
    _skip_no_docker()
    with RabbitMqContainer("rabbitmq:3.13-management-alpine") as container:
        host = container.get_container_host_ip()
        port = container.get_exposed_port(5672)
        yield f"amqp://guest:guest@{host}:{port}/"


# ── The pipeline's business logic — ONE pure function, shared by both
# transports. Using the same function in the sync and async relays is the
# parity guarantee: identical input events MUST produce identical enriched
# events regardless of transport. ────────────────────────────────────────


def make_dm_event(i: int) -> dict[str, Any]:
    """Deterministic mock Twitter DM (no randomness — verifiable)."""
    moods = ("love this!", "hate the delay", "when does it ship?")
    return {
        "id": f"dm-{i}",
        "sender_id": f"user-{i % 977}",
        "recipient_id": "brand-support",
        "text": f"  @Support {moods[i % 3]} order #{i} #help  ",
        "created_at": f"2026-07-08T00:00:{i % 60:02d}Z",
    }


def enrich(dm: dict[str, Any]) -> dict[str, Any]:
    """The relay's operation: normalize + extract + classify."""
    text = dm["text"].strip()
    words = text.split()
    lowered = text.lower()
    if "love" in lowered:
        sentiment = "positive"
    elif "hate" in lowered:
        sentiment = "negative"
    else:
        sentiment = "neutral"
    return {
        "id": dm["id"],
        "sender_id": dm["sender_id"],
        "recipient_id": dm["recipient_id"],
        "text": text,
        "created_at": dm["created_at"],
        "mentions": [w for w in words if w.startswith("@")],
        "hashtags": [w for w in words if w.startswith("#")],
        "sentiment": sentiment,
        "text_length": len(text),
    }


def expected_outputs() -> dict[str, dict[str, Any]]:
    return {f"dm-{i}": enrich(make_dm_event(i)) for i in range(N_EVENTS)}


def _verify(collected: dict[str, dict[str, Any]], duplicates: int, transport: str) -> None:
    expected = expected_outputs()
    missing = expected.keys() - collected.keys()
    unexpected = collected.keys() - expected.keys()
    assert not missing, f"[{transport}] LOST {len(missing)}/{N_EVENTS} events, e.g. {sorted(missing)[:5]}"
    assert not unexpected, f"[{transport}] unexpected ids: {sorted(unexpected)[:5]}"
    assert duplicates == 0, f"[{transport}] {duplicates} duplicate deliveries in a clean run"
    mismatched = [i for i, out in collected.items() if out != expected[i]]
    assert not mismatched, (
        f"[{transport}] {len(mismatched)} events enriched incorrectly, "
        f"e.g. {mismatched[0]}: got {collected[mismatched[0]]!r} "
        f"expected {expected[mismatched[0]]!r}"
    )


# ══════════════════════════════════════════════════════════════════════════
# Async pipeline
# ══════════════════════════════════════════════════════════════════════════


async def test_async_twitter_dm_pipeline_end_to_end(rabbitmq_url: str) -> None:
    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import BatchPublishConfig, ConnectionConfig, ConsumerConfig, RabbitConfig
    from rabbitkit.core.types import MessageEnvelope

    config = RabbitConfig(
        connection=ConnectionConfig.from_url(rabbitmq_url),
        consumer=ConsumerConfig(prefetch_count=200),
    )
    # batch_config: pipelined confirms for the high-volume producer path.
    broker = AsyncBroker(config, batch_config=BatchPublishConfig(batch_size=100, flush_interval_ms=20))

    collected: dict[str, dict[str, Any]] = {}
    duplicates = 0
    done = asyncio.Event()

    # Stage 1: relay — consume raw DM, enrich, forward. The @publisher result
    # path acks the source only after the enriched publish is CONFIRMED.
    @broker.subscriber(queue="dm.events.async")
    @broker.publisher(routing_key="dm.enriched.async")
    async def relay(body: bytes) -> bytes:
        return json.dumps(enrich(json.loads(body))).encode()

    # Stage 2: sink — collect and count.
    @broker.subscriber(queue="dm.enriched.async")
    async def sink(body: bytes) -> None:
        nonlocal duplicates
        event = json.loads(body)
        if event["id"] in collected:
            duplicates += 1
        collected[event["id"]] = event
        if len(collected) >= N_EVENTS:
            done.set()

    await broker.start()
    await asyncio.sleep(0.3)

    # Mock producer: N deterministic DM events, concurrent waves.
    t0 = time.monotonic()
    wave = 500
    for start in range(0, N_EVENTS, wave):
        outcomes = await asyncio.gather(*(
            broker.publish(
                MessageEnvelope(
                    routing_key="dm.events.async",
                    body=json.dumps(make_dm_event(i)).encode(),
                    message_id=f"dm-{i}",
                )
            )
            for i in range(start, min(start + wave, N_EVENTS))
        ))
        assert all(o.ok for o in outcomes), "producer publish failed"

    await asyncio.wait_for(done.wait(), timeout=PIPELINE_TIMEOUT)
    elapsed = time.monotonic() - t0
    await broker.stop()

    _verify(collected, duplicates, "async")
    # Two broker hops per event (ingest + enriched). Visible with -s / log_cli.
    logger.info(
        "[async pipeline] %d DMs through 2 stages in %.1fs (%.0f events/s end-to-end)",
        N_EVENTS, elapsed, N_EVENTS / elapsed,
    )


# ══════════════════════════════════════════════════════════════════════════
# Sync pipeline — same queues topology (own suffix), same enrich(), a
# worker-pool relay. Semantics must match the async run exactly.
# ══════════════════════════════════════════════════════════════════════════


def test_sync_twitter_dm_pipeline_end_to_end(rabbitmq_url: str) -> None:
    from rabbitkit.core.config import ConnectionConfig, ConsumerConfig, RabbitConfig, WorkerConfig
    from rabbitkit.core.types import MessageEnvelope
    from rabbitkit.sync.broker import SyncBroker

    config = RabbitConfig(
        connection=ConnectionConfig.from_url(rabbitmq_url),
        consumer=ConsumerConfig(prefetch_count=64),
    )
    broker = SyncBroker(config)

    collected: dict[str, dict[str, Any]] = {}
    duplicates = 0
    lock = threading.Lock()
    done = threading.Event()

    @broker.subscriber(queue="dm.events.sync")
    @broker.publisher(routing_key="dm.enriched.sync")
    def relay(body: bytes) -> bytes:
        return json.dumps(enrich(json.loads(body))).encode()

    @broker.subscriber(queue="dm.enriched.sync")
    def sink(body: bytes) -> None:
        nonlocal duplicates
        event = json.loads(body)
        with lock:
            if event["id"] in collected:
                duplicates += 1
            collected[event["id"]] = event
            if len(collected) >= N_EVENTS:
                done.set()

    broker.start(worker_config=WorkerConfig(worker_count=8))

    t0 = time.monotonic()
    for i in range(N_EVENTS):
        outcome = broker.publish(
            MessageEnvelope(
                routing_key="dm.events.sync",
                body=json.dumps(make_dm_event(i)).encode(),
                message_id=f"dm-{i}",
            )
        )
        assert outcome.ok, f"producer publish {i} failed: {outcome.status}"

    # Drive the consume loop on a background thread (the owner-thread
    # pattern used by the worker-pool integration tests).
    assert broker._transport is not None
    conn = broker._transport._connection
    consume_thread = threading.Thread(target=broker._transport.start_consuming, daemon=True)
    consume_thread.start()

    finished = done.wait(timeout=PIPELINE_TIMEOUT)
    elapsed = time.monotonic() - t0

    conn.add_callback_threadsafe(broker._transport.stop_consuming)
    consume_thread.join(timeout=10.0)
    broker.stop()

    assert finished, f"[sync] only {len(collected)}/{N_EVENTS} events arrived within {PIPELINE_TIMEOUT:.0f}s"
    _verify(collected, duplicates, "sync")
    logger.info(
        "[sync pipeline] %d DMs through 2 stages in %.1fs (%.0f events/s end-to-end)",
        N_EVENTS, elapsed, N_EVENTS / elapsed,
    )
