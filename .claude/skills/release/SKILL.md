---
description: Cut a rabbitkit release — bump the version in both places, finalize the changelog, and verify gates. Use when preparing a version bump.
argument-hint: <new-version, e.g. 0.7.1>
disable-model-invocation: true
---

Prepare release `$ARGUMENTS`. This is a deliberate, human-triggered action — never auto-invoked.

1. Bump the version in **both** places — they must match:
   - `src/rabbitkit/_version.py`
   - `pyproject.toml`
2. In `CHANGELOG.md`, move the `[Unreleased]` items under a new `[$ARGUMENTS]` heading with today's date; leave a fresh empty `[Unreleased]` section.
3. Run `/gates full` and confirm everything is green.
4. Show a summary diff of the version + changelog changes. Do NOT tag, commit, or push unless explicitly asked.
