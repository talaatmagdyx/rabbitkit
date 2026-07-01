# rabbitkit — Claude Code Context

## Commands

- `.venv/bin/pytest tests/unit/ -q --tb=short` — run unit tests
- `.venv/bin/pytest tests/unit/<pkg>/test_<module>.py -v` — run a single test file
- `.venv/bin/pytest --co -q tests/unit/<pkg>/` — list tests without running
- `.venv/bin/pytest tests/security/ tests/property/ -q --tb=short` — security regression scenarios + hypothesis property-based tests
- `.venv/bin/ruff check src/ tests/ benchmarks/` — lint check
- `.venv/bin/mypy src/rabbitkit/ --strict --ignore-missing-imports` — type check
- `pre-commit install` — one-time setup so `.pre-commit-config.yaml` (ruff + mypy) runs on `git commit`
- `python -m benchmarks` — run pipeline benchmarks

## Project Layout

- `src/rabbitkit/<package>/` — source; mirrors `tests/unit/<package>/`
- Each package has `__init__.py` that re-exports the public API
- Top-level `__init__.py` re-exports ALL public symbols
- New feature checklist: `src/`, `tests/unit/`, `__init__.py` exports, `CHANGELOG.md [Unreleased]`, `README.md`

## Architecture Rules

- **Shared core with ZERO transport imports** — `core/` never imports pika or aio-pika
- Sync transport: `sync/` (pika-based), Async transport: `async_/` (aio-pika-based)
- Config dataclasses: `@dataclass(frozen=True, slots=True)` — immutable by convention
- `RabbitConfig` only composes connection/broker defaults; throughput configs go to their components directly
- `WorkerConfig` is NOT part of `RabbitConfig` — passed to `broker.start(worker_config=)`

## Testing Patterns

- Test files: `tests/unit/<package>/test_<module>.py` (mirror source layout)
- Integration tests use `TestBroker` (in-memory, no RabbitMQ required)
- DI dependency factories must be at MODULE LEVEL (not inside test methods) because `from __future__ import annotations` makes all annotations lazy strings
- `@publisher` must be inner decorator (applied first), `@subscriber` outer (applied second)
- Coverage target: aim for 100%; add `# pragma: no cover` only for defensive guards

## Edit Rules

- `core/config.py` — frozen dataclasses, composable, no transport imports
- `core/types.py` — SINGLE canonical location for ALL enums and data types
- `core/topology.py` — Exchange/Queue models with validation; `to_declare_kwargs()` for transport
- `core/route.py` — `RouteDefinition` internal model; registration-time validation (fail fast)
- `core/pipeline.py` — Handler pipeline with middleware chain, DI, ack orchestration
- `sync/broker.py` / `async_/broker.py` — Broker wires registry + pipeline + transport
- `middleware/` — Each middleware is a separate module; re-exported via `__init__.py`
- `health.py` — `broker_health_check()` / `broker_health_check_async()`
- `streams.py` — `StreamOffset`, `StreamConsumerConfig`, `StreamOffsetType`

## Quality Gates

- `ruff check` — 0 warnings required
- `pytest` — all tests must pass
- `mypy --strict --ignore-missing-imports` — 0 errors required
- Version bumps: update `_version.py` AND `pyproject.toml`
