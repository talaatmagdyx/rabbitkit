# rabbitkit — Claude Code Context

## Commands

- `.venv/bin/pytest tests/unit/ -q --no-cov` — run unit tests
- `.venv/bin/pytest tests/unit/<pkg>/test_<module>.py -v` — run a single test file
- `.venv/bin/pytest --co -q tests/unit/<pkg>/` — list tests without running
- `make test` — run unit tests with coverage
- `make test-fast` — run unit tests without coverage
- `make lint` — run ruff linter
- `make typecheck` — run mypy type checker

## Project Layout

- `src/rabbitkit/<package>/` — source; mirrors `tests/unit/<package>/`
- Each package has `__init__.py` that re-exports the public API
- `core/` has ZERO transport imports (no pika, no aio-pika)
- `sync/` contains all pika-specific code
- `async_/` contains all aio-pika-specific code

## Architecture Rules

- **Core is transport-agnostic:** `core/` must NEVER import pika or aio-pika
- **Transport-specific exceptions:** Live in `sync/connection.py` and `async_/connection.py`, NOT in `core/errors.py`
- **Enums defined once:** All enums live in `core/types.py`, imported everywhere else
- **TopologyMode defined once:** In `core/types.py`, never duplicated
- **Config objects are frozen:** All `@dataclass(frozen=True, slots=True)`

## Testing Patterns

- Test files: `tests/unit/<package>/test_<module>.py` (mirror source layout)
- Use TestBroker for pipeline/middleware/DI tests — no RabbitMQ needed
- Coverage target is 100%
- All tests must pass without a running RabbitMQ broker

## Semantic Contracts

The plan file at `.claude/plans/melodic-sauteeing-sloth.md` contains 9 semantic contracts.
When contracts and code disagree, the contract wins — fix the code.

Key contracts:
- Contract 1: AckPolicy definitions (base + retry-override behavior)
- Contract 2: RabbitMessage sync/async ack
- Contract 3: Middleware ordering
- Contract 4: Parameter resolution precedence
- Contract 5: Result publishing precedence
- Contract 6: TopologyMode precedence
- Contract 7: Error classification default policy
- Contract 8: mandatory=True and returned messages
- Contract 9: Reconnect re-subscription
