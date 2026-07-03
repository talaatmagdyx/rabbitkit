# Production checklist

A scannable list of what to configure before trusting rabbitkit with real
traffic. Each item links to where it's explained in depth. This list is a
promoted, cleaned-up version of the checklist used internally during
rabbitkit's own production-readiness reviews.

## Delivery & retry

- [ ] Use **quorum queues** for money/order flows (`RabbitQueue(queue_type=QueueType.QUORUM, ...)`) with `delivery_limit=` set — a broker-enforced retry backstop that's completely independent of the (already-clamped, but still application-level) retry-count header. See [Retry & DLQ](../retry-and-dlq.md) and the Topology section of the [Full Guide](../guide/full-guide.md).
- [ ] Enable retry deliberately: `retry=RetryConfig(max_retries>=3, delays=(5, 30, 120, 600))`. Confirm the delay queues actually receive messages in a real environment, not just in `TestBroker` — `TestBroker` doesn't exercise real AMQP topology.
- [ ] Every consumer route has a DLQ. The default now guarantees this: `SafetyConfig.reject_without_dlx="auto_provision"` declares a `{queue}.dlq` for **every** route that can reject, not just retry-enabled ones. If you opt a route into `"discard"` you are explicitly accepting message loss; `"error"` fails startup instead for externally-managed topology. A manually-configured `dead_letter_exchange` is respected as-is, so if you set one yourself, make sure it's real.
- [ ] Transient errors on retry-less routes requeue forever by default (deliberate: "wait for the downstream to recover"). If a poison-pill hot-loop worries you more, opt into `ConsumerConfig(reject_transient_on_redelivery=True)` for a 2-strike cap to the DLQ.
- [ ] Read and apply **[the idempotency contract](idempotency.md)** — every handler that has a side effect must be safe to run more than once. This is the single most important item on this list.

## Publishing

- [ ] `PublisherConfig(confirm_delivery=True, persistent=True)` for anything durable. Don't use `confirm_delivery=False` (fire-and-forget) on a route with retry or a `@publisher` result — you'll get a `RuntimeWarning` at startup if you do, because a lost publish in that mode acks the source message anyway.
- [ ] Treat any `PublishOutcome` that isn't a real, confirmed delivery as a failure — `outcome.raise_for_status()` is the one-liner. If durability before settling something else matters, check `status == PublishStatus.CONFIRMED` specifically: with confirms off, `.ok` is also `True` for `SENT` (written to the socket, never broker-acknowledged).
- [ ] If you use `mandatory=True`, check for `PublishStatus.RETURNED` — an unroutable message is reported distinctly, not silently swallowed.
- [ ] **High-volume publishers use `AsyncBroker`.** Sync confirmed publish ceilings at ~0.9k msg/s (pika serializes confirms on one channel; `worker_count` does not raise it). `AsyncBroker` + `AsyncBatchPublisher` pipelines confirms (~6.1k msg/s measured); scale further with more processes. See the throughput note in the README.

## Concurrency & shutdown

- [ ] `ConsumerConfig(graceful_timeout=...)` must exceed your worst-case handler time. A handler that outlives it is **abandoned, not killed** — Python can't forcibly stop an arbitrary thread or a cancelled-but-still-running coroutine's cleanup, so size this generously rather than tightly.
- [ ] Kubernetes `terminationGracePeriodSeconds` must exceed `graceful_timeout` plus your `preStop` sleep, or the pod gets `SIGKILL`ed mid-message. See [the idempotency contract](idempotency.md) for why this is recoverable but noisy, not silently unsafe.
- [ ] Prefer `RabbitApp.run_async()` (or `asyncio.run(broker.run())`) over bare `await broker.start()` for async consumers — the latter's signal-handler-triggered drain is fire-and-forget and isn't guaranteed to finish before the process exits.
- [ ] `SyncBroker.stop()` must be called from the same thread that ran `start_consuming()` — exactly what `broker.run()` does. Don't wire your own cross-thread shutdown call.

## Connections

- [ ] **List every cluster node**: `ConnectionConfig(nodes=[...])` gives client-side failover to surviving nodes when the configured primary dies. A single `host=` against a multi-node cluster means one dead node takes your client down for the whole backoff window.
- [ ] **Publish-only `SyncBroker`** (no subscribers): call `broker.pump_idle()` periodically from your own idle loop. See [the sync-vs-async connection model](../guide/full-guide.md#sync-vs-async-two-different-connection-models) for why this only applies to sync.
- [ ] `ConnectionConfig(blocked_connection_timeout=...)` — fail fast on a blocked connection instead of appearing healthy for minutes while publishes silently stall.

## TLS & credentials

- [ ] `SSLConfig(enabled=True)` for any non-local broker. Defaults are already secure (`CERT_REQUIRED`, hostname verification, TLS ≥ 1.2) — don't weaken them without a specific reason.
- [ ] Never run with default `guest`/`guest` credentials off-box — rabbitkit warns on this, but don't ignore the warning. Load credentials from `SecretStr`/environment variables, and avoid logging a raw `ConnectionConfig`/`.url` — use `.safe_url` instead, which masks the password.
- [ ] Rotated/short-lived secrets (Vault, IAM): pass `ConnectionConfig(credentials_provider=...)` — re-resolved at every (re)connect, so rotation needs no redeploy. See [Security → Credential rotation](../security.md).
- [ ] Consumers don't need `configure` permission: declare topology once with a privileged credential, run services with `TopologyMode.PASSIVE_ONLY` and read/write-only permissions. See [Security → Least-privilege consumers](../security.md).

## Health / Kubernetes

- [ ] Wire **readiness** to `broker_readiness()` (connected + not blocked + consumers active) and **liveness** to `broker_liveness()` (process alive, not wedged) — and keep them distinct. Tying liveness to broker connectivity turns a transient RabbitMQ outage into a thundering-herd pod restart.
- [ ] Check `broker_health_check().blocked` — a connection can be `connected=True` and still `blocked=True` (RabbitMQ paused it via a memory/disk alarm). `broker_readiness()` already accounts for this; if you're building your own health logic on top of `broker_health_check()` directly, don't skip the `blocked` field.
- [ ] Multi-node cluster: pass `management_client=RabbitManagementClient(...)` to `broker_readiness()` — the process-local checks can't see a partition where your one connection is fine but the rest of the cluster isn't; the management check downgrades that to not-ready.
- [ ] See [`docs/kubernetes.md`](../kubernetes.md) for a full deployment manifest with probes, `PodDisruptionBudget`, and `preStop` wiring.

## Observability

- [ ] Scrape the emitted metrics (ack/nack/reject/retry/dead-letter counters, handler duration/errors). Don't build alerts on a metric name that isn't actually emitted — check `MetricsMiddleware`'s docs for the current list.
- [ ] **Wire `QueueMetricsPoller`** (bridges the management API into your `MetricsCollector`) — queue depth and consumer lag are the #1 RabbitMQ incident signal, and the consume/publish counters cannot see them. Without it, "DLQ depth > 0" below has no metric to alert on.
- [ ] Alert on rising `rabbitkit_messages_redelivered_total` (handlers dying/timing out before acking — crash loops, heartbeat kills) and `rabbitkit_reconnects_total` (flapping broker/network) — both invisible in the success/error counters.
- [ ] Avoid raw routing keys as metric labels if your routing keys embed IDs or tenant names — that's unbounded cardinality. Use the bound queue name or a static route pattern instead.
- [ ] Alert on: DLQ depth > 0, sustained retry rate, publish-confirm failures, consumer-active count == 0, connection blocked, worker-pool backlog.
- [ ] Structured logging: `LoggingConfig.redact_keys` is on by default and redacts credential-shaped fields in *your own* log calls, not just rabbitkit's internal ones (which never log bodies/headers to begin with). Don't disable it without a reason.

## Security

- [ ] If using message signing: wire a shared `RedisNonceCache` across every process/pod. The default in-memory cache gives you *no* real replay protection in a multi-process deployment — you'll get a `RuntimeWarning` if you skip this.
- [ ] If using the monitoring dashboard: `auth_token=` is not optional in anything beyond a local, loopback-only environment. See [Security](../security.md).
- [ ] Never construct `ManagementConfig.url` from user-controllable input.

## Before you call it done

- [ ] Run the real-broker integration suite (`pytest tests/integration/ -m integration`), not just unit tests against `TestBroker` — some correctness properties (real AMQP topology, real confirms, real quorum-queue delivery limits) can only be verified against an actual broker.
- [ ] Read [`docs/stability-policy.md`](../stability-policy.md) and confirm every symbol you depend on is in the tier you think it's in — an Experimental feature can change without a deprecation cycle.
