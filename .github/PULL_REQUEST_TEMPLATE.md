## Summary

<!-- What does this PR change, and why? Link the issue it addresses. -->

## Checklist

- [ ] `ruff check src/ tests/ benchmarks/` passes
- [ ] `mypy src/rabbitkit/ --strict --ignore-missing-imports` passes
- [ ] `pytest tests/unit/ --cov=src/rabbitkit --cov-report=term-missing` passes
- [ ] Tests added/updated in `tests/unit/<package>/test_<module>.py`
- [ ] `CHANGELOG.md` updated under `[Unreleased]`
- [ ] `README.md` / `docs/guide/full-guide.md` updated (if user-facing)
- [ ] If this touches transport/protocol behavior, verified against
      `tests/integration/` (real broker via testcontainers)

## Test plan

<!-- How did you verify this? What did you run, and what did it prove? -->
