"""Docs-example execution (Loop Engineering Review, Testing/Reliability).

Every ``` ```python ``` fenced block in README.md is checked so the examples
can't silently drift from actual behavior -- this is the exact class of gap
that let the C1 finding (retry=RetryConfig(...) declaring topology but never
installing the middleware) hide behind a passing test suite: nothing
executed the documented usage itself.

Three levels of checking, in increasing strictness:

1. Every block must be valid Python syntax (``ast.parse``) -- catches typos.
2. Every ``from rabbitkit... import X`` (and a few other doc-relevant
   packages) across every block must resolve to a real, importable symbol --
   catches an import-path rename that the README wasn't updated for (this
   is precisely the class of bug this test suite found while writing this
   file: the OLD README's FastAPI section imported from a module,
   ``rabbitkit.integrations.fastapi``, that does not exist).
3. The one block that is fully self-contained AND requires no real broker
   (the TestBroker Quick Start example) is actually executed end-to-end.

Blocks that need a live broker connection (``await broker.start()`` against
a real transport) are intentionally NOT executed here -- that's what
``tests/integration/`` with a real broker is for. Syntax + import checks
still cover those blocks.
"""

from __future__ import annotations

import ast
import importlib
import re
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[2]
_README = _REPO_ROOT / "README.md"

_PYTHON_BLOCK_RE = re.compile(r"```python\n(.*?)```", re.DOTALL)

# Packages whose import paths this test verifies -- rabbitkit itself, plus a
# couple of dependencies used directly in the Quick Start examples. Anything
# else (e.g. `redis`, a user's own module) is not checked, since it isn't
# rabbitkit's docs drifting.
_CHECKED_IMPORT_PREFIXES = ("rabbitkit",)


def _extract_python_blocks(markdown: str) -> list[str]:
    return [m.group(1) for m in _PYTHON_BLOCK_RE.finditer(markdown)]


def _iter_import_names(tree: ast.AST) -> list[tuple[str, str]]:
    """Yield (module, name) for every `from module import name` in tree."""
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                out.append((node.module, alias.name))
    return out


@pytest.fixture(scope="module")
def readme_blocks() -> list[str]:
    markdown = _README.read_text(encoding="utf-8")
    blocks = _extract_python_blocks(markdown)
    assert blocks, "expected at least one ```python block in README.md -- extraction regex may be broken"
    return blocks


class TestReadmeExampleSyntax:
    def test_every_block_is_valid_python(self, readme_blocks: list[str]) -> None:
        for i, block in enumerate(readme_blocks):
            try:
                ast.parse(block)
            except SyntaxError as exc:
                pytest.fail(f"README.md python block #{i} is not valid Python: {exc}\n\n{block}")


class TestReadmeExampleImportsResolve:
    def test_every_rabbitkit_import_resolves(self, readme_blocks: list[str]) -> None:
        """Every `from rabbitkit... import X` in every README code block
        must be a real, importable symbol. This is a static check (it does
        not require a broker connection), so it runs for every block
        including ones that also do `await broker.start()`.
        """
        failures: list[str] = []
        for i, block in enumerate(readme_blocks):
            tree = ast.parse(block)
            for module_name, symbol in _iter_import_names(tree):
                if not module_name.startswith(_CHECKED_IMPORT_PREFIXES):
                    continue
                try:
                    module = importlib.import_module(module_name)
                except ImportError as exc:
                    failures.append(f"block #{i}: `from {module_name} import {symbol}` -- module import failed: {exc}")
                    continue
                if not hasattr(module, symbol):
                    failures.append(
                        f"block #{i}: `from {module_name} import {symbol}` -- "
                        f"{module_name!r} has no attribute {symbol!r}"
                    )
        assert not failures, "README.md has stale import(s):\n" + "\n".join(failures)


class TestReadmeTestBrokerExampleRuns:
    def test_testbroker_quickstart_example_executes(self, readme_blocks: list[str]) -> None:
        """The one fully self-contained, no-real-broker-needed block --
        "Test it without RabbitMQ" -- is executed exactly as written, proving
        it isn't just syntactically valid but actually still works end to end.
        """
        candidates = [b for b in readme_blocks if "TestBroker" in b and "def test_" in b]
        assert candidates, "expected a TestBroker example block in README.md -- did the Quick Start section move?"
        block = candidates[0]

        namespace: dict[str, object] = {}
        exec(compile(block, "<README.md TestBroker example>", "exec"), namespace)  # noqa: S102

        test_fn = namespace.get("test_order_handler")
        assert test_fn is not None, "expected a test_order_handler() function in the TestBroker example block"
        test_fn()  # must not raise
