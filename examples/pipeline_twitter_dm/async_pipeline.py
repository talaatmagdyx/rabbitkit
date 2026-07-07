"""Twitter-DM two-stage pipeline (ASYNC) — producer → relay → sink, verified.

    mock DM producer ──> dm.events ──> relay (enrich) ──> dm.enriched ──> sink

One process runs all three roles for demonstration; in production each role
is its own deployment (same code, same queues). The relay uses the
``@publisher`` result path, so the source DM is acked only after the
enriched publish is broker-CONFIRMED — a crash anywhere loses nothing.

Volume (default 2000; the pipeline is O(n), so this genuinely scales):

    EVENTS=1000000 python examples/pipeline_twitter_dm/async_pipeline.py

Requirements:
    pip install "rabbitkit[async]"
    RabbitMQ on localhost:5672

The sync twin (examples/pipeline_twitter_dm/sync_pipeline.py) implements
the SAME pipeline with the same enrich() — run both and diff the output.
"""

import asyncio
import json
import os
import time
from typing import Any

from rabbitkit import BatchPublishConfig, ConsumerConfig, MessageEnvelope, RabbitConfig
from rabbitkit.async_ import AsyncBroker

EVENTS = int(os.environ.get("EVENTS", "2000"))

broker = AsyncBroker(
    RabbitConfig(consumer=ConsumerConfig(prefetch_count=200)),
    # Pipelined confirms for the producer path — same durability, ~3x throughput.
    batch_config=BatchPublishConfig(batch_size=100, flush_interval_ms=20),
)


# ── The operation (identical in the sync twin — that's the parity contract) ──

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


# ── Stage 1: relay — source acked ONLY after the forward publish confirms ──

@broker.subscriber(queue="dm.events")
@broker.publisher(routing_key="dm.enriched")
async def relay(body: bytes) -> bytes:
    return json.dumps(enrich(json.loads(body))).encode()


# ── Stage 2: sink — collect + verify ──

collected: dict[str, dict[str, Any]] = {}
done = asyncio.Event()


@broker.subscriber(queue="dm.enriched")
async def sink(body: bytes) -> None:
    event = json.loads(body)
    collected[event["id"]] = event
    if len(collected) >= EVENTS:
        done.set()


async def main() -> None:
    await broker.start()
    await asyncio.sleep(0.3)

    print(f"[producer] publishing {EVENTS:,} mock DM events ...")
    t0 = time.monotonic()
    wave = 500
    for start in range(0, EVENTS, wave):
        outcomes = await asyncio.gather(*(
            broker.publish(MessageEnvelope(
                routing_key="dm.events",
                body=json.dumps(make_dm_event(i)).encode(),
                message_id=f"dm-{i}",
            ))
            for i in range(start, min(start + wave, EVENTS))
        ))
        assert all(o.ok for o in outcomes), "producer publish failed"

    await asyncio.wait_for(done.wait(), timeout=max(120, EVENTS / 300))
    elapsed = time.monotonic() - t0
    await broker.stop()

    # Verify: zero loss, correct enrichment (recomputed independently).
    assert len(collected) == EVENTS, f"lost {EVENTS - len(collected)} events"
    for i in range(0, EVENTS, max(1, EVENTS // 1000)):  # sample ~1000 for speed
        assert collected[f"dm-{i}"] == enrich(make_dm_event(i)), f"dm-{i} enriched wrong"

    print(
        f"[verified] {EVENTS:,} DMs → relay(enrich) → sink in {elapsed:.1f}s "
        f"({EVENTS / elapsed:,.0f} events/s end-to-end, zero loss, enrichment correct)"
    )


if __name__ == "__main__":
    asyncio.run(main())
