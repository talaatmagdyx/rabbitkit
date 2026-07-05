"""Smoke-test every example script against a live broker.

Examples are documentation that executes — but nothing gated them, so they
drifted silently when the package API changed. This runner closes that gap:
the nightly Integration workflow runs it against a real RabbitMQ + Redis
(default ports 5672 / 6379, which is exactly what the examples connect to).

Classification per script, each given ``--timeout`` seconds:
  - PASS        exit 0 (a producer/demo that finishes)
  - RUNNING-OK  timed out still alive with no REAL error logged — i.e. a
                long-running consumer/server that started cleanly (the
                correct outcome for those; they never self-terminate).
                CancelledError / "event loop closed" from the kill itself
                is benign shutdown noise, not a failure.
  - FAIL        non-zero exit, or timed out with a real bug signature logged

Exit code is the number of FAILs, so CI fails loudly on real breakage while
tolerating the daemons that are supposed to run forever.

``SKIP`` lists package-internal modules that use relative imports
(``from .models import ...``) and therefore can only be imported, not run as
scripts. The package itself is import-checked separately.

Local use (needs RabbitMQ on 5672, Redis on 6379):
    python -m examples.smoke_test --timeout 15
"""

from __future__ import annotations

import argparse
import pathlib
import re
import subprocess
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
EXAMPLES = ROOT / "examples"

# Package-internal modules of the order_service example: relative imports mean
# they are not runnable standalone. `fastapi_app.py` is the runnable entrypoint
# and is NOT skipped.
SKIP = {
    "order_service/broker.py",
    "order_service/handlers.py",
    "order_service/services.py",
    "order_service/error_mapping.py",
    "order_service/config.py",
    "order_service/errors.py",
    "order_service/models.py",
    "order_service/dlq_tools.py",
    "order_service/management_tools.py",
    "smoke_test.py",
}

# A killed-at-timeout daemon unwinds its event loop and prints CancelledError /
# "Event loop is closed" / connection-teardown noise on stderr — that is NOT a
# failure, it's the correct consequence of stopping a long-running consumer.
# So match only REAL bug signatures: config/topology errors, bad imports, type
# errors, broker precondition failures. Anything else on a still-running
# process is benign shutdown noise → RUNNING-OK.
_REAL_ERROR = re.compile(
    r"ConfigurationError|DuplicateRouteError|PRECONDITION_FAILED"
    r"|ModuleNotFoundError|ImportError|AttributeError|TypeError|NameError"
    r"|KeyError|ValueError: .*(argument|keyword)",
    re.MULTILINE,
)


def _discover() -> list[pathlib.Path]:
    return sorted(
        p
        for p in EXAMPLES.rglob("*.py")
        if p.name != "__init__.py"
        and "__pycache__" not in p.parts
        and str(p.relative_to(EXAMPLES)) not in SKIP
    )


def _is_pytest_file(path: pathlib.Path) -> bool:
    """A pytest example (defines ``def test_*`` functions) must be run via
    pytest, not executed as a script — as a script it just defines functions
    and exits 0 (a hollow pass) or ImportErrors if pytest is absent."""
    text = path.read_text(errors="replace")
    return bool(re.search(r"^def test_\w+\(", text, re.MULTILINE))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--timeout", type=float, default=15.0, help="seconds per script")
    args = parser.parse_args()

    # order_service is a package, not a script — verify it imports cleanly
    # instead of executing its relative-import modules.
    try:
        import importlib

        for mod in ("broker", "handlers", "services", "fastapi_app"):
            importlib.import_module(f"examples.order_service.{mod}")
        print("PACKAGE-OK   order_service (imports clean)", flush=True)
    except Exception as e:
        print(f"PACKAGE-FAIL order_service: {e}", flush=True)
        sys.exit(1)

    fails: list[str] = []
    passes = running = 0
    for path in _discover():
        rel = str(path.relative_to(ROOT))
        # pytest examples run under pytest; plain scripts run directly.
        cmd = (
            [sys.executable, "-m", "pytest", rel, "-q", "-p", "no:cacheprovider"]
            if _is_pytest_file(path)
            else [sys.executable, rel]
        )
        try:
            cp = subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True, timeout=args.timeout)
            out = cp.stdout + cp.stderr
            if cp.returncode == 0:
                verdict, ok = "PASS", True
            else:
                verdict, ok = f"FAIL(rc={cp.returncode})", False
        except subprocess.TimeoutExpired as e:
            out = (e.stdout or b"").decode(errors="replace") + (e.stderr or b"").decode(errors="replace")
            if _REAL_ERROR.search(out):
                verdict, ok = "FAIL(hung+error)", False
            else:
                verdict, ok = "RUNNING-OK", True

        print(f"{verdict:<16} {rel}", flush=True)
        if not ok:
            fails.append(rel)
            # surface the first error line to make CI logs actionable
            m = _REAL_ERROR.search(out)
            if m:
                snippet = out[m.start():].splitlines()[:6]
                print("    " + "\n    ".join(snippet), flush=True)
        elif verdict == "PASS":
            passes += 1
        else:
            running += 1

    print("=" * 60, flush=True)
    print(f"pass={passes} running_ok={running} fail={len(fails)}", flush=True)
    sys.exit(len(fails))


if __name__ == "__main__":
    main()
