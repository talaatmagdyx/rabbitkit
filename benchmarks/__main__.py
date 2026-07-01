"""rabbitkit benchmark suite.

Starts ONE RabbitMQ container (or uses --url) and runs all benchmark suites
against it so Docker startup only pays once.  Results are persisted to
benchmarks/results/ and compared against the previous run.

Usage
-----
# Full suite with a shared container (requires Docker):
TESTCONTAINERS_RYUK_DISABLED=true python -m benchmarks

# Use an already-running RabbitMQ:
python -m benchmarks --url amqp://guest:guest@localhost/

# Run only a specific suite:
python -m benchmarks --only throughput
python -m benchmarks --only latency
python -m benchmarks --only failure
python -m benchmarks --only resources
python -m benchmarks --only lifecycle
python -m benchmarks --only pipeline
python -m benchmarks --only sync
"""

from __future__ import annotations

import argparse
import time


def _make_url(container: object) -> str:
    from benchmarks._common import make_url
    return make_url(container)


def main() -> None:
    parser = argparse.ArgumentParser(prog="python -m benchmarks", description="rabbitkit benchmark suite")
    parser.add_argument(
        "--url",
        default=None,
        help="AMQP URL for an existing RabbitMQ (default: auto-start testcontainers)",
    )
    parser.add_argument(
        "--only",
        choices=["throughput", "latency", "failure", "resources", "lifecycle", "pipeline", "sync"],
        help="Run only a specific suite",
    )
    parser.add_argument(
        "--no-save",
        action="store_true",
        help="Skip saving results to benchmarks/results/",
    )
    args = parser.parse_args()

    t_start = time.perf_counter()

    print()
    print("╔══════════════════════════════════════════════════════════════╗")
    print("║           rabbitkit Benchmark Suite                         ║")
    print("╚══════════════════════════════════════════════════════════════╝")
    print()

    suites = [args.only] if args.only else [
        "throughput", "latency", "failure", "resources", "lifecycle", "sync", "pipeline",
    ]

    # Load previous results before running so the comparison is stable
    from benchmarks._results import load_previous, print_delta, save
    previous = load_previous()

    all_results: dict[str, float] = {}

    def _run(url: str, container: object | None) -> None:
        for suite in suites:
            print()
            if suite == "throughput":
                from benchmarks.bench_throughput import run_all
                r = run_all(url)
                all_results.update(r or {})

            elif suite == "latency":
                from benchmarks.bench_latency import run_all
                r = run_all(url)
                all_results.update(r or {})

            elif suite == "failure":
                from benchmarks.bench_failure import run_all
                r = run_all(url)
                all_results.update(r or {})

            elif suite == "resources":
                from benchmarks.bench_resources import run_all
                run_all(url)  # does not return metrics dict yet

            elif suite == "lifecycle":
                from benchmarks.bench_lifecycle import _run_with_url
                _run_with_url(url, container)

            elif suite == "sync":
                from benchmarks.bench_sync import run_all
                r = run_all(url)
                all_results.update(r or {})

            elif suite == "pipeline":
                from benchmarks.bench_pipeline import run_all
                r = run_all()
                all_results.update(r or {})

    if args.url:
        _run(args.url, None)
    else:
        try:
            from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]
        except ImportError:
            print("ERROR: testcontainers not installed. Run: pip install testcontainers")
            print("       Or pass --url amqp://guest:guest@localhost/ for an existing broker.")
            return

        print("Starting RabbitMQ via testcontainers (rabbitmq:3.13-management-alpine)...")
        with RabbitMqContainer("rabbitmq:3.13-management-alpine") as c:
            url = _make_url(c)
            print(f"RabbitMQ ready at {url}")
            print()
            _run(url, c)

    elapsed = time.perf_counter() - t_start

    # Save results and show delta vs previous run
    if all_results and not args.no_save:
        path = save(all_results)
        if path:
            print(f"\n  Results saved to {path}")

    if all_results and previous:
        print_delta(all_results, previous)

    print()
    print(f"Total benchmark time: {elapsed:.1f}s")
    print()


if __name__ == "__main__":
    main()
