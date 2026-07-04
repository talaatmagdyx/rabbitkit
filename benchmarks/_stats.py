"""Statistical rigor for benchmarks — repetitions, robust stats, env fingerprint.

The single-run numbers in the classic suite are fine for smoke-level "did it
get 10x slower" checks, but useless for detecting real regressions on shared
CI runners (noise swamps signal). Everything here exists to make a number
worth comparing:

- ``robust()``: median (not mean — CI runners have heavy right tails), CV
  (coefficient of variation) so a reader can tell signal from noise at a
  glance, plus min/max spread.
- ``env_fingerprint()``: numbers without the machine they came from are
  not comparable; stored alongside every advanced-suite result.
"""

from __future__ import annotations

import os
import platform
import statistics
import subprocess
import sys
from typing import Any

# Coefficient-of-variation threshold above which a measurement should be
# treated as noise-dominated. 5% is strict for shared runners; the tables
# flag rather than hide unstable rows.
CV_UNSTABLE = 0.05


def robust(samples: list[float]) -> dict[str, float]:
    """Median-centred summary of repeated measurements."""
    if not samples:
        return {"n": 0, "median": 0.0, "mean": 0.0, "cv": 0.0, "min": 0.0, "max": 0.0}
    med = statistics.median(samples)
    mean = statistics.fmean(samples)
    stdev = statistics.stdev(samples) if len(samples) > 1 else 0.0
    return {
        "n": len(samples),
        "median": med,
        "mean": mean,
        "cv": (stdev / mean) if mean else 0.0,
        "min": min(samples),
        "max": max(samples),
    }


def fmt_rate(stats: dict[str, float]) -> str:
    """``median msg/s ±cv%`` with an instability flag."""
    flag = " ⚠" if stats["cv"] > CV_UNSTABLE else ""
    return f"{stats['median']:>10,.0f} ±{stats['cv'] * 100:>4.1f}%{flag}"


def percentiles(samples: list[float], points: tuple[float, ...] = (50, 95, 99, 99.9)) -> dict[str, float]:
    """Nearest-rank percentiles. Caller is responsible for sample-size sanity
    (a p99.9 needs ≥ ~10k samples to mean anything; the paced-latency bench
    collects enough or omits the point)."""
    if not samples:
        return {}
    s = sorted(samples)
    out: dict[str, float] = {}
    for p in points:
        idx = min(len(s) - 1, max(0, round(p / 100 * len(s)) - 1))
        out[f"p{p:g}"] = s[idx]
    out["max"] = s[-1]
    return out


def env_fingerprint() -> dict[str, Any]:
    """The machine context a result is only meaningful together with."""
    try:
        sha = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True, text=True, timeout=5, check=False,
        ).stdout.strip()
    except Exception:
        sha = "unknown"
    return {
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "machine": platform.machine(),
        "cpu_count": os.cpu_count(),
        "ci": bool(os.environ.get("CI")),
        "git_sha": sha,
    }
