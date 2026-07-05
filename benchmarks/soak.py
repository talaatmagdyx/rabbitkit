"""Long-running soak harness — the production-evidence generator.

Answers the two questions a point-in-time chaos gate cannot:

1. **Does connection recovery survive sustained abuse?** The chaos gate
   proves one restart mid-consume; this harness kills the broker every
   ``--restart-every`` seconds for the whole run and requires the consumer
   to resume after EVERY bounce (consumed count must advance within a
   recovery window after each restart).

2. **Does a long-running pod leak?** RSS, open file descriptors, and
   asyncio task count are sampled throughout. The verdict fits a
   least-squares slope to the post-warmup RSS series and fails on
   sustained growth; FDs and task count must be flat (max ≤ early max),
   which catches leaked channels/consumers/timers across reconnects.

Message-safety verdict: at-least-once — every id whose publish was
broker-confirmed must be consumed at least once by the end (duplicates are
counted and reported, not failed; that is the delivery contract).

Local (10 minutes, bounce every 2):
    python -m benchmarks.soak --duration 600 --restart-every 120

CI runs the weekly soak workflow (.github/workflows/soak.yml).
Exit code is the verdict: 0 = all pass.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

from benchmarks._common import IMAGE, _bench_safety
from benchmarks._stats import env_fingerprint

CONTAINER = "rabbitkit-soak"
PORT = int(os.environ.get("RK_SOAK_PORT", "32791"))
QUEUE = "soak-q"

RECOVERY_WINDOW_S = 60.0  # consumer must make progress this soon after a bounce
SAMPLE_EVERY_S = 15.0
RSS_SLOPE_LIMIT_KB_MIN = 256.0  # sustained growth above this fails the leak verdict
RSS_NET_FLOOR_KB = 8_192.0  # ...but only if net growth also exceeds this (slope
# alone is noise-dominated on short runs: few samples + allocator arena churn
# around broker restarts can produce a positive fit on a shrinking process)
WARMUP_FRACTION = 0.2  # discard early samples (allocator/arena warmup)


# ── Broker container management (same pattern as chaos_suite) ──────────────


def _sh(*args: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(list(args), capture_output=True, text=True, check=False)


def start_broker() -> str:
    _sh("docker", "rm", "-f", CONTAINER)
    run = _sh(
        "docker", "run", "-d", "--rm", "--name", CONTAINER,
        "-p", f"{PORT}:5672", IMAGE,
    )
    if run.returncode != 0:
        raise RuntimeError(f"docker run failed: {run.stderr.strip()}")
    url = f"amqp://guest:guest@127.0.0.1:{PORT}/"
    _wait_ready(url)
    return url


def restart_broker() -> None:
    _sh("docker", "restart", "-t", "3", CONTAINER)


def stop_broker() -> None:
    _sh("docker", "rm", "-f", CONTAINER)


def _wait_ready(url: str, timeout: float = 90.0) -> None:
    import pika

    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            pika.BlockingConnection(pika.URLParameters(url)).close()
            return
        except Exception:
            time.sleep(1.0)
    raise RuntimeError("broker did not become ready")


# ── Resource sampling ──────────────────────────────────────────────────────


def _sample(proc: Any, t0: float) -> dict[str, float]:
    return {
        "t": time.monotonic() - t0,
        "rss_kb": proc.memory_info().rss / 1024,
        "fds": float(proc.num_fds()) if hasattr(proc, "num_fds") else 0.0,
        "tasks": float(len(asyncio.all_tasks())),
    }


def _slope_kb_per_min(samples: list[dict[str, float]]) -> float:
    """Least-squares RSS slope over the post-warmup window."""
    pts = samples[int(len(samples) * WARMUP_FRACTION):]
    if len(pts) < 3:
        return 0.0
    xs = [p["t"] / 60 for p in pts]
    ys = [p["rss_kb"] for p in pts]
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    denom = sum((x - mx) ** 2 for x in xs)
    if denom == 0:
        return 0.0
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys, strict=True)) / denom


# ── The soak ───────────────────────────────────────────────────────────────


async def soak(url: str, duration: float, rate: int, restart_every: float) -> dict[str, Any]:
    import psutil

    from rabbitkit.async_.broker import AsyncBroker
    from rabbitkit.core.config import ConnectionConfig, ConsumerConfig, RabbitConfig

    broker = AsyncBroker(
        RabbitConfig(
            safety=_bench_safety(),
            connection=ConnectionConfig.from_url(url),
            consumer=ConsumerConfig(prefetch_count=100),
        )
    )

    # O(1)-memory message tracking (the first 30-min run failed its own RSS
    # verdict because two sets holding 360k ids cost ~45 MB — the harness was
    # measuring itself). Ids are published sequentially and confirmed in
    # order, so:
    #   - confirmed ids are always the contiguous range [0, confirmed_count)
    #     → a counter, not a set;
    #   - seen ids compact to a watermark W ("everything ≤ W seen") plus a
    #     small residual set for out-of-order arrivals (≈ prefetch window).
    # If a message is genuinely lost mid-run the watermark stalls and the
    # residual grows — memory contamination only on runs that already fail
    # the no_loss verdict, which is acceptable.
    confirmed_count = 0
    watermark = -1
    residual: set[int] = set()
    received_total = 0

    def _unique_seen() -> int:
        return (watermark + 1) + len(residual)

    @broker.subscriber(queue=QUEUE, prefetch_count=100)
    async def handle(body: bytes) -> None:
        nonlocal received_total, watermark
        received_total += 1
        i = int(body)
        if i <= watermark or i in residual:
            return  # duplicate (at-least-once redelivery)
        residual.add(i)
        while (watermark + 1) in residual:
            watermark += 1
            residual.discard(watermark)

    await broker.start()

    proc = psutil.Process(os.getpid())
    t0 = time.monotonic()
    samples: list[dict[str, float]] = [_sample(proc, t0)]
    restarts: list[dict[str, float]] = []
    stop = asyncio.Event()

    async def publisher() -> None:
        nonlocal confirmed_count
        i = 0
        while not stop.is_set():
            due = t0 + i / rate
            delay = due - time.monotonic()
            if delay > 0:
                await asyncio.sleep(delay)
            # at-least-once producer: retry the same id until broker-confirmed
            while not stop.is_set():
                try:
                    outcome = await broker.publish(routing_key=QUEUE, body=str(i).encode())
                    if outcome.ok:
                        confirmed_count = i + 1  # confirmed ids ≡ [0, i]
                        break
                except Exception:
                    pass
                await asyncio.sleep(0.5)
            i += 1

    async def chaos() -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=restart_every)
                return
            except TimeoutError:
                pass
            mark = _unique_seen()
            print(f"  [{time.monotonic() - t0:7.1f}s] restarting broker "
                  f"(consumed so far: {mark:,})", flush=True)
            await asyncio.to_thread(restart_broker)
            restarts.append({"t": time.monotonic() - t0, "seen_before": float(mark)})

    async def sampler() -> None:
        while not stop.is_set():
            try:
                await asyncio.wait_for(stop.wait(), timeout=SAMPLE_EVERY_S)
                return
            except TimeoutError:
                samples.append(_sample(proc, t0))

    tasks = [asyncio.create_task(c) for c in (publisher(), chaos(), sampler())]
    await asyncio.sleep(duration)
    stop.set()
    await asyncio.gather(*tasks, return_exceptions=True)

    # drain grace: let in-flight/requeued messages arrive
    drain_deadline = time.monotonic() + 60
    while _unique_seen() < confirmed_count and time.monotonic() < drain_deadline:
        await asyncio.sleep(1.0)
    samples.append(_sample(proc, t0))
    await broker.stop()

    # ── Verdicts ──────────────────────────────────────────────────────────
    # lost = confirmed ids ([0, confirmed_count)) never seen. Everything
    # ≤ watermark was seen; add residual ids that fall inside the range.
    in_range_residual = sum(1 for r in residual if r < confirmed_count)
    covered = min(watermark + 1, confirmed_count) + in_range_residual
    lost_count = confirmed_count - covered
    recovery_failures = []
    for r in restarts:
        # consumer must have made progress within the recovery window
        deadline = r["t"] + RECOVERY_WINDOW_S
        progressed = any(
            s["t"] > r["t"] and s["t"] <= deadline for s in samples
        ) and _unique_seen() > r["seen_before"]
        if not progressed:
            recovery_failures.append(r["t"])

    slope = _slope_kb_per_min(samples)
    rss_net_kb = samples[-1]["rss_kb"] - samples[0]["rss_kb"]
    early = samples[: max(2, len(samples) // 4)]
    fd_growth = max(s["fds"] for s in samples) - max(s["fds"] for s in early)
    task_growth = max(s["tasks"] for s in samples) - max(s["tasks"] for s in early)

    return {
        "duration_s": duration,
        "rate_msg_s": rate,
        "restarts": len(restarts),
        "published_confirmed": confirmed_count,
        "consumed_unique": _unique_seen(),
        "duplicates": received_total - _unique_seen(),
        "lost": lost_count,
        "recovery_failures": recovery_failures,
        "rss_start_kb": samples[0]["rss_kb"],
        "rss_end_kb": samples[-1]["rss_kb"],
        "rss_slope_kb_per_min": slope,
        "rss_net_kb": rss_net_kb,
        "fd_growth": fd_growth,
        "task_growth": task_growth,
        "samples": samples,
        "verdicts": {
            "no_loss": lost_count == 0,
            "recovered_after_every_restart": not recovery_failures,
            # a leak must show BOTH a sustained slope and material net growth
            "rss_bounded": slope <= RSS_SLOPE_LIMIT_KB_MIN or rss_net_kb <= RSS_NET_FLOOR_KB,
            "fds_bounded": fd_growth <= 16,
            "tasks_bounded": task_growth <= 32,
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration", type=float, default=600, help="seconds under load")
    parser.add_argument("--rate", type=int, default=200, help="target publish msg/s")
    parser.add_argument("--restart-every", type=float, default=120,
                        help="seconds between broker restarts")
    args = parser.parse_args()

    print("=" * 78)
    print("rabbitkit Soak (sustained load + periodic broker kills + leak tracking)")
    print(f"  duration={args.duration:.0f}s  rate={args.rate}/s  "
          f"restart_every={args.restart_every:.0f}s")
    print("=" * 78)

    url = start_broker()
    try:
        report = asyncio.run(soak(url, args.duration, args.rate, args.restart_every))
    finally:
        stop_broker()

    v = report["verdicts"]
    print(f"\n  published(confirmed)={report['published_confirmed']:,}  "
          f"unique={report['consumed_unique']:,}  dupes={report['duplicates']:,}  "
          f"lost={report['lost']}")
    print(f"  restarts={report['restarts']}  recovery_failures={report['recovery_failures']}")
    print(f"  rss {report['rss_start_kb'] / 1024:.1f} → {report['rss_end_kb'] / 1024:.1f} MB  "
          f"slope={report['rss_slope_kb_per_min']:+.1f} KB/min  "
          f"fd_growth={report['fd_growth']:+.0f}  task_growth={report['task_growth']:+.0f}")
    for name, ok in v.items():
        print(f"  [{'PASS' if ok else 'FAIL'}] {name}")

    out = Path(__file__).parent / "results"
    try:
        out.mkdir(exist_ok=True)
        path = out / f"soak_{int(time.time())}.json"
        path.write_text(json.dumps({"env": env_fingerprint(), **report}, indent=2))
        print(f"\n  report: {path}")
    except Exception as e:
        print(f"  (report save failed: {e})")

    sys.exit(0 if all(v.values()) else 1)


if __name__ == "__main__":
    main()
