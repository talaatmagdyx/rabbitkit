#!/usr/bin/env bash
# Stop hook — block turn-end until the quality gates pass.
#
# Only runs the gates when Python source/test files have uncommitted changes,
# so question-and-answer turns (no code touched) stop instantly. Fail-fast: the
# first failing gate blocks and tells Claude what to fix. Claude Code ends the
# turn anyway after 8 consecutive blocks, so a gate that can't be satisfied
# won't loop forever.
set -uo pipefail
cd "${CLAUDE_PROJECT_DIR:-.}" 2>/dev/null || true
cat >/dev/null   # drain the hook's JSON stdin; no field is needed

changed=$( { git diff --name-only HEAD -- src tests
             git ls-files -o --exclude-standard -- src tests; } 2>/dev/null | grep -E '\.py$' )
[ -z "$changed" ] && exit 0   # nothing to verify -> allow stop

block() {   # emit {"decision":"block","reason":...} so Claude keeps working
  printf '%s' "$1" | jq -Rs '{decision:"block",reason:.}'
  exit 0
}

if ! o=$(.venv/bin/ruff check src/ tests/ benchmarks/ 2>&1); then
  block "Quality gate FAILED: ruff. Fix every issue below, then finish.
$o"
fi
if ! o=$(.venv/bin/mypy src/rabbitkit/ --strict --ignore-missing-imports 2>&1); then
  block "Quality gate FAILED: mypy --strict. Fix every error below, then finish.
$o"
fi
if ! o=$(.venv/bin/pytest tests/unit/ -q --tb=short 2>&1); then
  block "Quality gate FAILED: pytest unit. Make the suite green, then finish.
$(printf '%s' "$o" | tail -40)"
fi
exit 0   # all gates green -> allow stop
