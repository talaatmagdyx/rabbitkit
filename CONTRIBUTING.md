# Contributing to rabbitkit

Thanks for considering a contribution. This document covers everything you
need to get a change from idea to merged PR.

## Code of Conduct

This project follows the [Contributor Covenant](CODE_OF_CONDUCT.md). By
participating, you're expected to uphold it.

## Getting started

```bash
git clone https://github.com/talaatmagdy/rabbitkit.git
cd rabbitkit
python -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

For the integration suite (requires a running Docker daemon):

```bash
pip install -e ".[integration]"
```

## Project layout

- `src/rabbitkit/<package>/` — source; each package mirrors
  `tests/unit/<package>/`.
- `core/` is transport-free — it never imports `pika` or `aio-pika`, directly
  or transitively. Transport-specific code belongs in `sync/` (pika) or
  `async_/` (aio-pika).
- Each package's `__init__.py` re-exports its public API; the top-level
  `src/rabbitkit/__init__.py` re-exports everything public, alphabetically
  sorted in `__all__`.

## Making a change

1. **Open an issue first** for anything beyond a small fix, so the approach
   can be discussed before you invest time in an implementation.
2. **Write the code** in `src/rabbitkit/...`, following the existing style in
   that module (config dataclasses are `@dataclass(frozen=True, slots=True)`;
   see `core/config.py` for examples).
3. **Add tests** in `tests/unit/<package>/test_<module>.py`, mirroring the
   source layout. Prefer `TestBroker` (in-memory, no RabbitMQ required) for
   integration-style tests; real-broker tests live in `tests/integration/`
   and need Docker.
4. **Update docs** — `CHANGELOG.md` under `[Unreleased]`, and `README.md` /
   `docs/guide/full-guide.md` if the change is user-facing.
5. **Run the quality gates** (see below) before opening a PR.

## Quality gates

All of these must pass:

```bash
.venv/bin/ruff check src/ tests/ benchmarks/
.venv/bin/mypy src/rabbitkit/ --strict --ignore-missing-imports
.venv/bin/pytest tests/unit/ -q --tb=short --cov=src/rabbitkit --cov-report=term-missing
.venv/bin/pytest tests/security/ tests/property/ -q --tb=short
```

- `ruff check` — zero warnings.
- `mypy --strict --ignore-missing-imports` — zero errors.
- `pytest tests/unit/` — all unit tests pass. The project targets ~100%
  coverage; CI's floor is 85% (`--cov-fail-under=85`) so it stays green
  while a few defensive/transport-shim paths are brought under test — new
  code should still aim for full coverage, with `# pragma: no cover`
  reserved for genuinely unreachable defensive guards.
- `pytest tests/security/ tests/property/` — security regression scenarios
  (signing replay, decompression bombs) and hypothesis property-based
  round-trip tests; all must pass.

`pre-commit install` (one-time) runs `ruff --fix` and `mypy --strict` on
`git commit` via `.pre-commit-config.yaml`, mirroring CI's `lint` job so
failures surface before you push.

If you touched transport/protocol behavior, also run the integration suite
against a real broker:

```bash
.venv/bin/pytest tests/integration/ -m integration -q --tb=short
```

## Testing conventions

- DI dependency factories must be defined at **module level**, not inside
  test methods — `from __future__ import annotations` makes all annotations
  lazy strings, so a factory nested in a test function won't resolve.
- Handler modules that decode a Pydantic body must **not** use
  `from __future__ import annotations` — the pipeline reads raw
  `inspect.signature` annotations.
- Decorator order matters: `@publisher` is the inner decorator (applied
  first), `@subscriber` is outer (applied second).

## Commit / PR style

- Keep commits focused; a bug fix doesn't need to also refactor unrelated
  code.
- PR description should explain *why*, not just *what* — link the issue it
  addresses.
- CI runs lint, type-check, unit tests (all matrix Python versions), and the
  integration suite. All must be green before merge.

## Reporting security issues

Please do **not** open a public issue for a security vulnerability — see
[SECURITY.md](SECURITY.md) for the private reporting process.
