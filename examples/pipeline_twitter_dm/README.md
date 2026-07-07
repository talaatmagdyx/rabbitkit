# Twitter-DM pipeline — the same two-stage pipeline, sync and async

A realistic event pipeline, implemented twice with **identical semantics**:

```
mock DM producer ──> dm.events ──> relay (enrich) ──> dm.enriched ──> sink (verify)
```

- **Producer**: publishes N deterministic mock Twitter DM events (JSON).
- **Relay**: consumes each DM, enriches it (normalize text, extract
  mentions/hashtags, classify sentiment), and forwards to the second queue
  via the `@publisher` result path — the source DM is acked **only after**
  the enriched publish is broker-confirmed, so a crash anywhere in the
  chain loses nothing.
- **Sink**: collects enriched events and the script verifies zero loss and
  byte-for-byte correct enrichment before exiting 0.

Both files share the same `enrich()` — that's the parity contract: the sync
and async pipelines produce identical output for identical input. The CI
version of this scenario (testcontainers, strict assertions, both
transports) lives in `tests/integration/test_pipeline_twitter_dm.py`.

## Run

```bash
docker run -d -p 5672:5672 rabbitmq:3.13-management-alpine

python examples/pipeline_twitter_dm/async_pipeline.py           # 2,000 events
python examples/pipeline_twitter_dm/sync_pipeline.py            # 1,000 events

EVENTS=1000000 python examples/pipeline_twitter_dm/async_pipeline.py   # go big
```

## Measured (local Docker broker, one process running all three roles)

| Pipeline | Volume | End-to-end |
|---|---|---|
| async | 100,000 DMs | ~2,100 events/s, zero loss |
| sync  | 1,000 DMs   | ~780 events/s, zero loss |

The gap is the documented sync ceiling (worker publishes marshal one
confirm at a time through the I/O thread — `docs/production/scale.md` §3).
In production you'd run each role as its own deployment and scale the
relay by pods; see the sizing table in the scale handbook.
