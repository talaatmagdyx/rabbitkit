---
paths:
  - "tests/**/*.py"
---

# Testing rules

- Mirror source layout: `tests/unit/<package>/test_<module>.py` for `src/rabbitkit/<package>/<module>.py`.
- Integration tests use `TestBroker` (in-memory) — no real RabbitMQ required. Real-broker tests live in `tests/integration/` and need a running broker (`127.0.0.1`, not `localhost`).
- DI dependency factories must be at **module level**, not inside test methods — `from __future__ import annotations` makes all annotations lazy strings, so nested factories won't resolve.
- Handler modules that decode a Pydantic body must NOT use `from __future__ import annotations` (the pipeline reads raw `inspect.signature` annotations).
- Decorator order: `@publisher` inner (applied first), `@subscriber` outer (applied second).
- Coverage target is 100%. Add `# pragma: no cover` only for defensive guards that can't be hit in a test.
- Run a single file: `.venv/bin/pytest tests/unit/<pkg>/test_<module>.py -v`. List without running: `.venv/bin/pytest --co -q tests/unit/<pkg>/`.
