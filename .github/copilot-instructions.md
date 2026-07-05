The canonical agent guide for this repo is **AGENTS.md** at the repo root —
read it first for the hard invariants, gates, layout, and gotchas.

Quick rules: `src/rabbitkit/core/` never imports `pika`/`aio_pika`; one handler
per queue; run ruff + `mypy --strict` + `pytest tests/unit/` before committing;
no `Co-Authored-By` trailers; don't bump the version unless asked.
