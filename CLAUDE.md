# rabbitkit — Claude Code Context

The canonical agent guide for this repo is **@AGENTS.md** — read it first.
It has the hard invariants, gates, layout, and the gotchas that make you fast.
Everything below is Claude-Code-specific and additive.

## Claude Code notes

- When adding a public symbol, follow the new-feature checklist in AGENTS.md.
- Reference code as `file_path:line` — it's clickable in the CLI.
- Prefer the dedicated file/search tools over shell `cat`/`grep`.
