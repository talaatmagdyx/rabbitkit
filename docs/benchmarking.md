# Benchmarking methodology

rabbitkit ships two benchmark tiers with different jobs. Knowing which
question each can answer — and which it cannot — matters more than any
single number.

## Tier 1: the classic suite (`python -m benchmarks`)

Single-pass smoke benchmarks: throughput drain, closed-loop latency,
failure-path overhead, lifecycle timings, resource tracking. Runs in CI's
best-effort step and the nightly workflow.

**What it is for:** catching order-of-magnitude regressions ("consume got
10x slower") and verifying every path *works* under load.

**What it is NOT for:** detecting small regressions or comparing
serializers. Its payloads are ~9 bytes, so every scenario is AMQP
round-trip-bound — raw / JSON / Pydantic / msgspec all measure the same
thing (which is why their numbers agree). Its numbers are single runs on
shared CI vCPUs; a ±10% swing between runs is noise, not signal.

## Tier 2: the advanced suite (`python -m benchmarks.advanced`)

Repeated, statistically-reported measurements designed for real
performance questions. ~10 minutes; runs nightly and on demand
(`--quick` for a 2-rep functional pass).

### Overhead A/B (`bench_overhead`)

The toolkit's headline cost: identical work through bare `aio-pika` and
through rabbitkit's full pipeline (middleware chain, DI, AUTO ack),
**interleaved** raw/kit/raw/kit so runner drift biases both sides
equally, 5 reps, reported as median ± CV with an instability flag when
CV exceeds 5%.

### Dimension sweeps (`bench_matrix`)

- **Payload sizes** 100 B / 4 KB / 64 KB — shows where the workload flips
  from per-message overhead-bound to bandwidth-bound (msg/s falls while
  MB/s climbs).
- **Classic vs quorum queues** — the production checklist mandates quorum;
  this measures what that choice costs on the consume path.

### Open-loop paced latency (`bench_matrix`)

The classic latency bench is closed-loop: the publisher paces itself by
its own progress on the same event loop as the consumer, which hides
queueing delay (*coordinated omission*). The paced bench publishes on an
**absolute schedule** (`t0 + i/rate`) and measures latency from the
*intended* send time, so scheduler lag and queue buildup are charged to
the system under test. It also reports the publisher's own schedule lag
(p99) — if the load generator couldn't hold the pace, the report says so
instead of publishing flattering numbers.

## Tier 2b: bulk vs single (`python -m benchmarks.bench_bulk`)

Answers "what do `publish_many` / `ack_many` / `CoalescingAcker` actually buy
over one call per message?" — with the correctness accounting every
throughput number must carry (every input has a reported state; the queue is
verified drained through the management API).

| Scenario | What it does | Median msg/s | Wire frames / msg | Accounting |
|---|---|---|---|---|
| publish single / async | `await broker.publish(e)` one after another | **932** | 1 publish + 1 confirm | `.ok` per call |
| publish gather / async ×10 | hand-rolled `asyncio.gather` under a 10-slot semaphore | 4,685 | same | none (you write it) |
| `publish_many` ×10 (default) | default options → in-flight capped to the 10-channel pool | 4,548 | same | one `BulkPublishItem` per input |
| `publish_many` ×64 | `PoolConfig(channel_pool_size=64)`, `max_in_flight=64` | 8,951 | same | same |
| `publish_many` + `AsyncBatchPublisher` | batch config, in-flight 256 (pipelined confirms) | **9,862** | same | same |
| publish single / sync | `SyncBroker.publish` one after another | 833 | same | `.ok` per call |
| `publish_many` / sync | `SyncBroker.publish_many` (sequential by design) | 1,015 | same | one item per input |
| ack each | `await msg.ack_async()` per delivery | 15,308 | 1.000 | — |
| `ack_many` ×100 | park 100, one `ack_many` call | 11,808 | 1.000 (20 API calls) | one `SettlementItem` per delivery |
| `CoalescingAcker` ×100 | register/complete, in-order completion | **15,670** | **0.010** (20 frames for 2,000) | coordinator ledger |

Environment: Apple Silicon (arm64, 12 cores), Python 3.12.2, RabbitMQ 3.13
in Docker on the same machine, 2,000 messages × 1 KiB, persistent,
confirms on, `mandatory=True`, durable classic queue, median of 3 runs,
git `ed2e8bc`. Every scenario reported 2,000/2,000 CONFIRMED (or settled)
and the management API showed the queue drained to 0 ready / 0 unacked.
Absolute numbers are this machine's; the ratios are what travel.

**How to read it**

- **Bulk publish is a concurrency story, not a wire story.** RabbitMQ has no
  multi-message publish; `publish_many` sends one message per envelope and
  pipelines confirms. Sequential `publish` is RTT-bound (~0.9k/s). The same
  concurrency by hand (`gather` ×10) gets ~4.7k/s, and `publish_many` ×10
  matches it — the accounting layer costs nothing measurable. Raising the
  channel pool to 64 doubles it again; the batch publisher (confirms
  pipelined on shared channels) reaches ~9.9k/s, about 10× sequential.
- **Sync bulk is honestly not faster.** pika cannot pipeline confirms, so
  `SyncBroker.publish_many` runs at the same ~1k/s as a loop. What you gain
  is the per-item outcome contract, not throughput. Use `AsyncBroker` (or
  `SyncBatchPublisher`) for volume.
- **`ack_many` is not a throughput feature.** It still emits one
  `multiple=False` frame per delivery — by design, so it can never settle a
  sibling that is still running — and it awaits them sequentially inside one
  call, which is why it measures slightly below per-handler acks that
  overlap with consumption. Its value is one validation pass, one report and
  one API call per batch commit (`STALE`/`DUPLICATE`/`ALREADY_SETTLED` per
  item), i.e. correctness after a database transaction.
- **Coalescing is the wire optimisation.** `CoalescingAcker` settled 2,000
  deliveries with 20 cumulative frames (0.01 frames/msg, 100× fewer) at the
  highest settle rate — because the coordinator knows every outstanding
  delivery and only acks through a completed prefix. Out-of-order
  completion lowers the ratio (stranded tags fall back to individual acks
  after `max_hold` rounds); in-order completion is the ceiling shown here.
- Handler cost was zero in the ack scenarios, so those rates are a
  **settlement-overhead ceiling**, not an end-to-end consume rate.

```bash
# throwaway testcontainers broker (Docker required)
python -m benchmarks.bench_bulk --n 2000 --size 1024 --reps 3
# or an existing broker
python -m benchmarks.bench_bulk --url amqp://guest:guest@localhost:5672/ --mgmt-url http://localhost:15672
```

Results (with an environment fingerprint) land in
`benchmarks/results/bulk_<timestamp>.json`.

## Tier 3: the soak harness (`python -m benchmarks.soak`)

Sustained-runtime evidence for the two questions point-in-time tests
cannot answer — the top risks for a long-running Kubernetes consumer:

- **Connection recovery under sustained abuse.** The broker container is
  killed every `--restart-every` seconds for the entire run; the verdict
  requires the consumer to make progress within 60 s of *every* bounce,
  and every broker-confirmed publish to be consumed at least once by the
  end (duplicates are counted and reported — that's the at-least-once
  contract — but loss fails the run).
- **Leak detection.** RSS, open file descriptors, and asyncio task count
  are sampled every 15 s. The verdict fits a least-squares slope to the
  post-warmup RSS series (fails above 256 KB/min sustained) and requires
  FD/task counts to stay flat across all the reconnect cycles — leaked
  channels, consumers, or timers show up here.

Runs weekly (`.github/workflows/soak.yml`, 30 min with a kill every 3
minutes ≈ 10 recovery cycles; dispatchable with custom duration) and
locally: `python -m benchmarks.soak --duration 600 --restart-every 120`.
The exit code is the verdict; the JSON report (with full sample series
and environment fingerprint) uploads as a workflow artifact.

## Reading the numbers

- Prefer **medians**; CI runners have heavy right tails.
- A result with `⚠` (CV > 5%) is noise-dominated — rerun or compare only
  against results from the same machine.
- Every advanced result JSON (`benchmarks/results/advanced_*.json`)
  embeds an environment fingerprint (python, platform, CPU count, git
  SHA, CI flag). Numbers without their machine are not comparable.
- Absolute msg/s across different machines is meaningless; **ratios**
  (overhead %, quorum/classic, size-to-size) travel well.

## Known limitations (deliberate, documented)

- No CPU pinning / turbo control — impossible on hosted runners; the
  interleaved A/B design compensates where it matters most.
- Steady-state producer/consumer equilibrium is not measured (drain
  bursts are a consume *ceiling*, labeled as such).
- No flamegraph capture; profile locally with `py-spy record -- python
  -m benchmarks.advanced --quick --url ...` when a regression needs
  explaining.
