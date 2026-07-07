"""Twitter-DM two-stage pipeline (SYNC) — the exact same pipeline as the
async twin, on ``SyncBroker`` with a worker pool.

    mock DM producer ──> dm.events.sync ──> relay (enrich) ──> dm.enriched.sync ──> sink

Same ``enrich()`` as async_pipeline.py — identical inputs MUST produce
identical enriched outputs regardless of transport; that is the parity
contract. Same safety property too: the relay's source DM is acked only
after the forwarded publish is broker-confirmed.

Expect materially lower end-to-end throughput than the async twin: worker
publishes marshal one confirm at a time through the connection's I/O
thread (see docs/production/scale.md §3) — that is the documented sync
ceiling on display, not a bug. Scale sync by processes, or use async.

Volume:  EVENTS=100000 python examples/pipeline_twitter_dm/sync_pipeline.py

Requirements:
    pip install "rabbitkit[sync]"
    RabbitMQ on localhost:5672
"""

import json
import os
import threading
import time
from typing import Any

from rabbitkit import ConsumerConfig, MessageEnvelope, RabbitConfig, WorkerConfig
from rabbitkit.sync import SyncBroker

EVENTS = int(os.environ.get("EVENTS", "1000"))

broker = SyncBroker(RabbitConfig(consumer=ConsumerConfig(prefetch_count=64)))


# ── Identical to the async twin — the parity contract ──

def make_dm_event(i: int) -> dict[str, Any]:
    moods = ("love this!", "hate the delay", "when does it ship?")
    return {
        "id": f"dm-{i}",
        "sender_id": f"user-{i % 977}",
        "recipient_id": "brand-support",
        "text": f"  @Support {moods[i % 3]} order #{i} #help  ",
        "created_at": f"2026-07-08T00:00:{i % 60:02d}Z",
    }


def enrich(dm: dict[str, Any]) -> dict[str, Any]:
    text = dm["text"].strip()
    words = text.split()
    lowered = text.lower()
    sentiment = "positive" if "love" in lowered else "negative" if "hate" in lowered else "neutral"
    return {
        **{k: dm[k] for k in ("id", "sender_id", "recipient_id", "created_at")},
        "text": text,
        "mentions": [w for w in words if w.startswith("@")],
        "hashtags": [w for w in words if w.startswith("#")],
        "sentiment": sentiment,
        "text_length": len(text),
    }


# ── Stage 1: relay (runs on worker-pool threads; settlement is marshalled) ──

@broker.subscriber(queue="dm.events.sync")
@broker.publisher(routing_key="dm.enriched.sync")
def relay(body: bytes) -> bytes:
    return json.dumps(enrich(json.loads(body))).encode()


# ── Stage 2: sink ──

collected: dict[str, dict[str, Any]] = {}
lock = threading.Lock()
done = threading.Event()


@broker.subscriber(queue="dm.enriched.sync")
def sink(body: bytes) -> None:
    event = json.loads(body)
    with lock:
        collected[event["id"]] = event
        if len(collected) >= EVENTS:
            done.set()


def main() -> None:
    broker.start(worker_config=WorkerConfig(worker_count=8))

    print(f"[producer] publishing {EVENTS:,} mock DM events ...")
    t0 = time.monotonic()
    for i in range(EVENTS):
        outcome = broker.publish(MessageEnvelope(
            routing_key="dm.events.sync",
            body=json.dumps(make_dm_event(i)).encode(),
            message_id=f"dm-{i}",
        ))
        assert outcome.ok, f"producer publish {i} failed: {outcome.status}"

    # Drive the consume loop on this (owner) thread's background twin, then
    # wait for the sink to see everything.
    assert broker._transport is not None
    conn = broker._transport._connection
    consume_thread = threading.Thread(target=broker._transport.start_consuming, daemon=True)
    consume_thread.start()

    finished = done.wait(timeout=max(120, EVENTS / 100))
    elapsed = time.monotonic() - t0

    conn.add_callback_threadsafe(broker._transport.stop_consuming)
    consume_thread.join(timeout=10)
    broker.stop()

    assert finished, f"only {len(collected)}/{EVENTS} events arrived"
    for i in range(0, EVENTS, max(1, EVENTS // 1000)):
        assert collected[f"dm-{i}"] == enrich(make_dm_event(i)), f"dm-{i} enriched wrong"

    print(
        f"[verified] {EVENTS:,} DMs → relay(enrich) → sink in {elapsed:.1f}s "
        f"({EVENTS / elapsed:,.0f} events/s end-to-end, zero loss, enrichment correct)"
    )


if __name__ == "__main__":
    main()
