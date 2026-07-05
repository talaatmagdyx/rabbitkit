# AGENTS.md — rabbitkit

Canonical guide for AI coding agents (Codex, Claude Code, Cursor, Copilot,
etc.). Tool-specific files (`CLAUDE.md`, `.cursor/rules/`,
`.github/copilot-instructions.md`) all point here — edit **this** file only.

## What this is

rabbitkit is a production-grade RabbitMQ toolkit for Python: sync (pika) and
async (aio-pika) brokers, decorator routing, retry/DLQ, middleware, DI, RPC,
and an in-memory `TestBroker`. Public beta on PyPI (`pip install rabbitkit`).

## Hard invariants — break these and CI fails

1. **`core/` imports zero transport.** Nothing under `src/rabbitkit/core/`
   may import `pika` or `aio_pika`, directly or transitively. Transport code
   lives in `sync/` or `async_/`. This is enforced and load-bearing.
2. **One handler per queue.** `@broker.subscriber(queue="x")` twice on the
   same queue raises `DuplicateRouteError`. Use one handler + `filter_fn`, or
   separate queues bound to one exchange.
3. **Config dataclasses are `@dataclass(frozen=True, slots=True)`** —
   immutable, composable. `core/types.py` is the ONE place for all enums.
4. **Run the gates before every commit** (see below). Zero ruff warnings,
   zero mypy errors, all tests green — non-negotiable.
5. **No `Co-Authored-By` / AI-attribution trailers** in commits or PRs.
6. **Version bumps touch BOTH** `src/rabbitkit/_version.py` and
   `pyproject.toml`. Don't bump version unless explicitly asked.

## Gates (must all pass before committing)

```bash
.venv/bin/pytest tests/unit/ -q --tb=short
.venv/bin/ruff check src/ tests/ benchmarks/
.venv/bin/mypy src/rabbitkit/ --strict --ignore-missing-imports
```

Single test file: `.venv/bin/pytest tests/unit/<pkg>/test_<module>.py -v`
Docs build: `.venv/bin/mkdocs build --strict`

## Layout

- `src/rabbitkit/core/` — transport-free: config, types, topology, route,
  pipeline, message, errors. Shared by both transports.
- `src/rabbitkit/sync/` (pika) · `src/rabbitkit/async_/` (aio-pika) —
  broker + transport adapters over the same core.
- `src/rabbitkit/middleware/` — one module each (retry, dedup, metrics, otel,
  compression, rate limit, timeout, circuit breaker, signing).
- `tests/unit/<pkg>/test_<module>.py` mirrors `src/rabbitkit/<pkg>/<module>.py`.
- `examples/` — 25 runnable projects (not in the wheel; CI-gated nightly).
- `benchmarks/` — classic suite + `advanced.py` (A/B overhead, sweeps) + `soak.py`.

## Gotchas that waste agent time (learn these first)

- **The safety-DLX 406.** `SafetyConfig.reject_without_dlx="auto_provision"`
  (the default) re-declares a queue with an `x-dead-letter-exchange` arg. If
  something already declared that queue *plain* (a preloader, a prior run, a
  test), the broker rejects it: `PRECONDITION_FAILED - inequivalent arg`. In
  benchmarks/tests, pass `reject_without_dlx="discard"` or declare the DLX
  topology first, then declare passively.
- **Stale durable queues poison local runs.** A reused local broker
  accumulates durable queues; a "conflict" is usually residue, not a bug.
  Verify on a fresh broker (`docker run --rm rabbitmq:3.13-management-alpine`)
  before believing a topology failure is real.
- **Only ONE broker on :5672.** A Homebrew `rabbitmq` service and a Docker
  broker both binding 5672 silently shadow each other — the classic
  "passes locally, fails in CI" cause. Check `lsof -nP -i :5672`.
- **README code blocks are executed by tests.** `tests/unit/test_readme_examples.py`
  AST-parses and import-checks every `python` block and runs the TestBroker
  one. No top-level `await` in README snippets; wrap in a function.
- **Sync transport is owner-thread-bound.** Every `SyncBroker`/transport call
  must come from the thread that called `start()`. Settlement from worker
  threads is marshalled internally. See `docs/concurrency-model.md`.
- **Sync consumers use `broker.run()`**, not bare `start()` — `run()` blocks,
  reconnects, and drains on SIGTERM. `start()` alone doesn't reconnect.
- **DI dependency factories must be module-level** — `from __future__ import
  annotations` makes annotations lazy strings, so nested factories won't
  resolve. Decorator order: `@publisher` inner, `@subscriber` outer.
- **pytest examples run via pytest**, not `python file.py` (they'd exit 0
  without running). The examples smoke test handles this automatically.

## New-feature checklist

`src/` + mirrored `tests/unit/` + `__init__.py` re-exports (package and
top-level) + `CHANGELOG.md [Unreleased]` + `README.md` + docs page if public.

## Docs worth reading

`docs/concurrency-model.md` (threading rules), `docs/benchmarking.md` (the two
benchmark tiers), `docs/migrating-to-rabbitkit.md`, `docs/stability-policy.md`
(what gates 1.0). API docs build from Google-style docstrings via mkdocstrings.
