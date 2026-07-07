# Troubleshooting

Symptom → likely cause → fix. If you don't see your problem here, check
[`docs/guide/full-guide.md`](guide/full-guide.md) for the feature you're
using, or open an issue.

## Connection issues

**"Connection refused" / can't connect at all.**
Check `ConnectionConfig.host`/`port`/`vhost` against the actual broker.
Remember RabbitMQ's default management UI port (`15672`) is different from
the AMQP port (`5672`) — a common copy-paste mistake. If you're inside
Docker Compose or Kubernetes, confirm you're using the service name, not
`localhost`.

**Connection appears healthy but publishes silently stall.**
The connection is likely **blocked** by a RabbitMQ memory or disk alarm —
this is a real, distinct state from "disconnected." Check
`broker_health_check(broker).blocked`. Set
`ConnectionConfig(blocked_connection_timeout=...)` so a blocked connection
fails fast instead of stalling for minutes. See
[Health Checks](guide/full-guide.md#13-health-checks).

**A publish-only `SyncBroker` keeps needing to reconnect.**
Nothing is driving the connection's I/O loop between publishes — see
[the sync-vs-async connection model](guide/full-guide.md#sync-vs-async-two-different-connection-models).
Call `broker.pump_idle()` periodically from your own idle loop.

## Messages stuck in retry / not reaching the DLQ

**Messages seem to retry forever, or the delay queues never receive anything.**
Confirm `retry=` is actually set (broker-wide or per-route) — this is what
declares the delay-queue topology *and* installs the retry middleware
together. If you manually added a `RetryMiddleware` to `middlewares=[...]`
without also setting `retry=` on the same route, the topology was never
declared and the middleware's delay-queue publishes are targeting
non-existent queues (you'll get a loud warning for this specific
misconfiguration).

**A message is redelivered indefinitely and never dead-letters.**
Check whether it's being reclassified as `TRANSIENT` on every attempt after
already exhausting `max_retries` — this should not happen (exhausted
retries dead-letter, they don't get re-retried), but if you've overridden
error classification with a custom `unknown_policy` or predicate, verify it
doesn't override the exhausted-attempt path. For a broker-enforced backstop
independent of any application logic, use a quorum queue with
`delivery_limit=`.

**The DLQ is growing and I don't know why.**
Use `DLQInspector.peek("queue.dlq", limit=10)` to look without consuming.
Check the message's `x-death` header (standard RabbitMQ dead-letter
metadata) for the original queue and reason. If it's a `filter_fn`
rejection, the auto-declared `<queue>.dlq` and its `RuntimeWarning` at
startup will tell you it exists — that's expected, not a leak, but you
should still be draining/alerting on it.

## Consumer not receiving messages

**Handler never fires, no errors.**
Check the routing key / binding actually matches what's being published —
this is usually a topic-exchange wildcard mismatch (`orders.*` doesn't match
`orders.created.eu`, `orders.#` does). Verify with
`rabbitkit topology list myapp.main:broker` (requires `rabbitkit[cli]`) or
the RabbitMQ management UI's bindings view.

**`consumer_count` is 0 in a health check even though the process is running.**
The consumer channel likely died without the connection itself closing
(e.g. a channel-level protocol error). `broker_readiness()` cross-checks
consumer registration against actual channel liveness for exactly this
case — if it reports not-ready, trust it over "the process looks fine."

## Idle / heartbeat disconnects

**A low-traffic consumer gets marked "dead" by liveness checks, or
reconnects unexpectedly during quiet periods.**
This was a real gap fixed in liveness heartbeat handling: the heartbeat is
now driven by the I/O loop tick (sync: once per `start_consuming()`
iteration; async: a periodic background task), not just message delivery —
so a healthy, quiet consumer stays "alive." If you're on an older version
or still seeing this, check `wedged_timeout` on `broker_liveness()` is set
generously relative to your actual traffic pattern.

**A publish-only broker's connection drops after being idle.**
Expected without `pump_idle()` — see the connection issues section above.

**The consumer channel closes mid-handler with "delivery acknowledgement
… timed out" (PRECONDITION_FAILED).**
RabbitMQ force-closes a channel if a delivered message stays unacked past
the server's `consumer_timeout` (default **30 minutes**) — the classic
silent killer for long-running handlers with `AckPolicy.MANUAL` or a
generous/disabled `TimeoutMiddleware`. The server does **not** advertise
this limit to clients, so rabbitkit can't warn you up front. If a handler
can legitimately hold a message longer, raise the limit per queue at
declaration time — `RabbitQueue(name=..., consumer_timeout=3_600_000)`
(ms; RabbitMQ ≥ 3.12) — or ack earlier and track completion elsewhere.

**Publish fails with `MessageTooLargeError` (a `ValueError` subclass):
`Message body … exceeds PublisherConfig.max_message_bytes`.**
Working as intended: the client-side guard defaults to **16 MiB**,
mirroring RabbitMQ's own `max_message_size` default. Without the guard the
server rejects the message anyway — but with a channel exception that kills
the (pooled) publisher channel and corrupts sibling in-flight publishes.
If you deliberately raised `max_message_size` in `rabbitmq.conf`, set
`PublisherConfig(max_message_bytes=...)` to match (the server doesn't
advertise its limit, so rabbitkit can't discover it); `0` disables the
guard. Better: store large payloads externally and publish a reference.

## Installation

**`ModuleNotFoundError: No module named 'pkg_resources'` when importing
`aio-pika` (or rabbitkit's async transport).**
`aio-pika==9.0.0` specifically imports `pkg_resources`, which recent
`setuptools` releases (>=81) no longer ship. This is an `aio-pika` packaging
issue, not a rabbitkit one — rabbitkit requires `aio-pika>=9.1.0` precisely
because of this; `9.1.0` and later don't have the problem. If you've pinned
`aio-pika` to exactly `9.0.0` yourself (or an old lockfile resolved to it),
bump it to `9.1.0` or later.

## Testing

**A bug reproduces against a real broker but not `TestBroker`.**
This is a known limitation, not a bug in `TestBroker` — it's an in-memory
fake that never speaks real AMQP, so it can't catch a bug in the transport
layer or real RabbitMQ topology (delay-queue TTL/DLX wiring, quorum-queue
delivery limits, real publisher confirms). Reproduce it in
`tests/integration/` against a real broker (via `testcontainers` — no
manually-managed broker required, see
[Real-broker integration tests](guide/full-guide.md#25-testing)).

**Assertions on `TestBroker.assert_acked()`/`assert_nacked()` don't match
what I expect.**
Remember `TestBroker`'s settlement is real, not mocked — if your handler
raises and your `AckPolicy` is `AUTO`, the message is genuinely nacked or
rejected per the same classification logic production uses. If the
assertion is surprising, the classification (not the assertion) is usually
the thing to check first.

## Security / signing

**`InvalidSignatureError` on messages that look correctly signed.**
Check `content_encoding`, `routing_key`, `exchange`, and `reply_to` — the
default signature covers all of these, not just the body. Republishing a
captured message under a different routing key, or flipping compression on
after signing, invalidates the signature by design.

**Replay protection doesn't seem to work across multiple workers/pods.**
Expected with the default nonce cache — it's per-process. Wire
`nonce_cache=RedisNonceCache(redis.Redis(...))` to share the seen-set. You
should already be seeing a `RuntimeWarning` about this if you haven't.

## Still stuck?

Check [`docs/guide/full-guide.md`](guide/full-guide.md) for the feature in
depth, [`docs/stability-policy.md`](stability-policy.md) to confirm you're
using an API in the tier you think it's in, or open an issue with a minimal
reproduction.
