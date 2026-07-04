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
