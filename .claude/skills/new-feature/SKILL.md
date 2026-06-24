---
description: Scaffold a new rabbitkit feature end to end — source, mirrored tests, public API exports, changelog, and docs. Use when adding a new module or public symbol.
argument-hint: <feature-name>
---

Add the feature `$ARGUMENTS` to rabbitkit. Follow the full new-feature checklist — do not stop at the source file:

1. **Source** — `src/rabbitkit/<package>/<module>.py`. If it touches the shared core, keep it transport-free (no pika / aio-pika). Sync code → `sync/`, async → `async_/`.
2. **Tests** — `tests/unit/<package>/test_<module>.py`, mirroring the source path. Aim for 100% coverage.
3. **Exports** — re-export the public API from the package `__init__.py` AND the top-level `src/rabbitkit/__init__.py`.
4. **CHANGELOG.md** — add an entry under `[Unreleased]`.
5. **README.md** — document it if it's user-facing.

Then run `/gates` and report the result.
