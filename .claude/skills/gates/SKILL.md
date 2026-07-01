---
description: Run all rabbitkit quality gates — ruff, mypy --strict, and the unit suite. Use before committing or when asked to verify the project is green.
argument-hint: "[unit|full]  (default: unit)"
---

Run rabbitkit's quality gates in order and report a single pass/fail summary. Stop at the first hard failure and show its output.

1. `.venv/bin/ruff check src/ tests/ benchmarks/` — must be 0 warnings.
2. `.venv/bin/mypy src/rabbitkit/ --strict --ignore-missing-imports` — must be 0 errors.
3. `.venv/bin/pytest tests/unit/ -q --tb=short` — all must pass.
4. `.venv/bin/pytest tests/security/ tests/property/ -q --tb=short` — security regression scenarios and hypothesis property-based tests; all must pass.

If `$ARGUMENTS` is `full`, also run the integration suite (`.venv/bin/pytest tests/integration/ -q`) — it needs a real broker on `127.0.0.1`; skip with a note if none is reachable.

Report as a table: gate, status (✅/⛔), and the failing detail if any.
