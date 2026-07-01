"""Benchmark result persistence — save runs and compare against the previous.

Results are stored as JSON in benchmarks/results/run_<timestamp>.json.
Each file contains a flat dict of {metric_name: float} for all suites.

Usage in __main__.py:
    prev = load_previous()
    # ... run benchmarks, collect results dict ...
    save(results)
    print_delta(results, prev)
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

RESULTS_DIR = Path(__file__).parent / "results"


def save(data: dict[str, float]) -> Path | None:
    """Write results to benchmarks/results/run_<timestamp>.json."""
    try:
        RESULTS_DIR.mkdir(exist_ok=True)
        ts = int(time.time())
        path = RESULTS_DIR / f"run_{ts}.json"
        with open(path, "w") as f:
            json.dump({"timestamp": ts, "metrics": data}, f, indent=2)
        return path
    except Exception:
        return None


def load_previous() -> dict[str, float] | None:
    """Return metrics from the most recent prior run, or None."""
    if not RESULTS_DIR.exists():
        return None
    files = sorted(RESULTS_DIR.glob("run_*.json"))
    if not files:
        return None
    try:
        with open(files[-1]) as f:
            obj: Any = json.load(f)
        return obj.get("metrics", {})
    except Exception:
        return None


def print_delta(current: dict[str, float], previous: dict[str, float] | None) -> None:
    """Print a compact regression table vs the previous run."""
    if not previous:
        return

    throughput_keys = ("_msg_s",)
    latency_keys = ("_ms", "_ns")

    rows = []
    for key, val in sorted(current.items()):
        prev_val = previous.get(key)
        if prev_val is None or prev_val == 0:
            continue
        pct = (val - prev_val) / abs(prev_val) * 100
        higher_better = any(key.endswith(k) for k in throughput_keys)
        lower_better = any(k in key for k in latency_keys) and not higher_better

        if higher_better:
            trend = "+" if pct > 2 else ("-" if pct < -2 else "~")
        elif lower_better:
            trend = "+" if pct < -2 else ("-" if pct > 2 else "~")
        else:
            trend = "~"

        rows.append((trend, key, val, prev_val, pct))

    if not rows:
        return

    print()
    print("  vs previous run:")
    print(f"  {'Metric':<52}  {'now':>12}  {'prev':>12}  {'delta':>8}")
    print(f"  {'-'*52}  {'-'*12}  {'-'*12}  {'-'*8}")
    for trend, key, val, prev_val, pct in rows:
        sign = "+" if pct >= 0 else ""
        print(f"  [{trend}] {key:<48}  {val:>12,.1f}  {prev_val:>12,.1f}  {sign}{pct:>6.1f}%")
    print()
    print("  [+] improvement  [-] regression  [~] stable (within 2%)")
