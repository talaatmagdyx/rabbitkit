"""Advanced benchmark runner — overhead A/B + matrix sweeps, with stats.

Separate from ``python -m benchmarks`` (the classic smoke suite): these runs
repeat measurements, interleave A/B comparisons, and take ~10 minutes — meant
for the nightly workflow and local performance work, not the per-push gate.

    python -m benchmarks.advanced                    # testcontainers broker
    python -m benchmarks.advanced --url amqp://...   # existing broker
    python -m benchmarks.advanced --quick            # 2 reps, for smoke

Results land in benchmarks/results/advanced_<ts>.json together with an
environment fingerprint — a number without the machine it came from is not
comparable to anything.
"""

from __future__ import annotations

import argparse
import json
import time
from pathlib import Path
from typing import Any

from benchmarks._stats import env_fingerprint

RESULTS_DIR = Path(__file__).parent / "results"


def run(url: str, reps: int) -> dict[str, Any]:
    from benchmarks import bench_matrix, bench_overhead

    metrics: dict[str, float] = {}
    metrics.update(bench_overhead.run_all(url, reps=reps))
    metrics.update(bench_matrix.run_all(url))
    return {"timestamp": int(time.time()), "env": env_fingerprint(), "metrics": metrics}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default=None)
    parser.add_argument("--quick", action="store_true", help="2 reps instead of 5")
    args = parser.parse_args()
    reps = 2 if args.quick else 5

    if args.url:
        payload = run(args.url, reps)
    else:
        from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

        from benchmarks._common import IMAGE, make_url

        with RabbitMqContainer(IMAGE) as c:
            payload = run(make_url(c), reps)

    try:
        RESULTS_DIR.mkdir(exist_ok=True)
        path = RESULTS_DIR / f"advanced_{payload['timestamp']}.json"
        path.write_text(json.dumps(payload, indent=2))
        print(f"\nResults saved to {path}")
    except Exception as e:
        print(f"\n(result save failed: {e})")


if __name__ == "__main__":
    main()
