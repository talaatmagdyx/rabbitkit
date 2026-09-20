"""Integration-suite gate.

The problem this closes: every module here calls ``pytest.skip()`` when
Docker or testcontainers is missing, and **pytest exits 0 when every test
skips**. Exit code 5 means "nothing was collected", which is not the same
thing — skipped tests *are* collected. So a CI job whose Docker had gone
away reported a green real-broker gate having executed nothing at all.

That is the same failure mode that hid the OpenTelemetry middleware being
untested for months: a skip is silent, and silence reads as success.

Set ``RK_REQUIRE_BROKER=1`` (CI does) and this file turns the whole class of
silent no-op into a loud failure:

* missing prerequisites abort the session immediately with a clear reason,
  instead of degrading into a pile of skips;
* a STRUCTURAL skip (no Docker, no testcontainers) fails the run, because it
  means the broker was never exercised;
* a run that passes zero tests fails the run.

Environment-bound skips *inside* a test that did reach a real broker stay
legal — some scenarios need ``rabbitmqctl`` operations that not every host
permits. Those are reported, not fatal.

Locally the variable is unset, so skipping still works the way a developer
without Docker expects.
"""

from __future__ import annotations

import asyncio
import contextlib
import os
import time
from collections.abc import Iterator
from typing import Any

import pytest

REQUIRE_BROKER_ENV = "RK_REQUIRE_BROKER"


def _broker_is_required() -> bool:
    return os.environ.get(REQUIRE_BROKER_ENV) == "1"


def _prerequisite_failure() -> str | None:
    """Return a human reason the real-broker suite cannot run, or None."""
    try:
        import testcontainers.rabbitmq  # type: ignore[import-untyped]  # noqa: F401
    except ImportError:
        return "testcontainers is not installed (pip install 'testcontainers[rabbitmq]')"
    try:
        import docker  # type: ignore[import-untyped]

        docker.from_env().ping()
    except Exception as exc:  # pragma: no cover - environment dependent
        return f"the Docker daemon is not reachable: {exc}"
    return None


#: Substrings identifying a skip that means "the broker was never reached".
#: Anything else is an environment-bound skip inside a test that DID run.
_STRUCTURAL_SKIP_MARKERS = ("testcontainers", "Docker daemon")


def _is_structural_skip(report: Any) -> bool:
    """True when the skip means no broker was exercised at all."""
    longrepr = getattr(report, "longrepr", None)
    reason = longrepr[2] if isinstance(longrepr, tuple) and len(longrepr) == 3 else str(longrepr)
    return any(marker in reason for marker in _STRUCTURAL_SKIP_MARKERS)


def pytest_sessionstart(session: pytest.Session) -> None:
    """Fail fast when the gate is required but cannot possibly run."""
    if not _broker_is_required():
        return
    reason = _prerequisite_failure()
    if reason is not None:
        pytest.exit(
            f"{REQUIRE_BROKER_ENV}=1 but the real-broker suite cannot run: {reason}. "
            "Refusing to report a green integration gate that executed nothing.",
            returncode=1,
        )


@pytest.hookimpl(trylast=True)
def pytest_sessionfinish(session: pytest.Session, exitstatus: int) -> None:
    """A required gate must actually have exercised the broker."""
    if not _broker_is_required():
        return
    reporter: Any = session.config.pluginmanager.get_plugin("terminalreporter")
    if reporter is None:  # pragma: no cover - always present under pytest
        return
    skipped = reporter.stats.get("skipped", [])
    passed = len(reporter.stats.get("passed", []))
    structural = [r for r in skipped if _is_structural_skip(r)]

    if structural:
        names = ", ".join(sorted({r.nodeid.split("::")[-1] for r in structural})[:5])
        reporter.write_line("")
        reporter.write_line(
            f"ERROR: {REQUIRE_BROKER_ENV}=1 but {len(structural)} integration test(s) skipped for "
            f"want of a broker: {names}. A skipped real-broker test proves nothing.",
            red=True,
        )
        session.exitstatus = 1
    elif passed == 0:
        reporter.write_line("")
        reporter.write_line(
            f"ERROR: {REQUIRE_BROKER_ENV}=1 but no integration test passed. "
            "The gate must exercise a real broker.",
            red=True,
        )
        session.exitstatus = 1


# ══════════════════════════════════════════════════════════════════════════
# Shared broker + fast liveness probes
#
# Three things used to dominate this suite's wall clock, none of them a real
# assertion:
#
# 1. Eleven separate RabbitMQ containers (~4s of startup each).
# 2. The management API, which refreshes queue statistics on a ~5s interval —
#    so every "wait until the queue drained" poll paid a structural 5s before
#    it could possibly see the truth. No broker setting shortens it.
# 3. Fixed ``sleep(0.3)`` calls standing in for "the consumer is registered".
#
# What follows replaces all three with one shared container and two live,
# sub-second probes: ``rabbitmqctl list_queues`` (~0.3s, never stale) for
# ready/unacked, and a passive ``queue_declare`` (~0.01s) for ready count and
# consumer count. Assertions are untouched — only the waiting is.
# ══════════════════════════════════════════════════════════════════════════

def skip_without_docker() -> None:
    """Skip with a STRUCTURAL reason (see ``_STRUCTURAL_SKIP_MARKERS``)."""
    reason = _prerequisite_failure()
    if reason is not None:
        pytest.skip(f"real-broker suite unavailable: {reason}")


@pytest.fixture(scope="session")
def rabbit_container() -> Iterator[dict[str, Any]]:
    """ONE RabbitMQ container for the whole integration suite.

    Yields ``{"url", "mgmt", "container"}``. Module fixtures delegate here
    instead of starting their own broker; ``container`` is exposed because
    several probes shell into the node with ``rabbitmqctl``.

    SERIAL ONLY — do not run this suite under pytest-xdist while sharing
    this container. Some tests use node-wide operations: ``rabbitmqctl
    close_all_connections`` and the management ``close_connection`` behind
    ``_kill_connections`` drop EVERY connection on the node, not just the
    caller's, so a parallel test would be collateral damage.

    ``test_blocked_connection_watchdog_closes_on_alarm`` deliberately does
    NOT use this fixture: it raises a node-wide disk alarm that blocks every
    publisher, and its restore is not ``finally``-guarded.
    """
    skip_without_docker()
    from testcontainers.rabbitmq import RabbitMqContainer  # type: ignore[import-untyped]

    with RabbitMqContainer("rabbitmq:3.13-management-alpine").with_exposed_ports(15672) as container:
        host = container.get_container_host_ip()
        yield {
            "url": f"amqp://guest:guest@{host}:{container.get_exposed_port(5672)}/",
            "mgmt": f"http://{host}:{container.get_exposed_port(15672)}",
            "container": container,
        }


# ── live queue counts (rabbitmqctl, ~0.3s, never stale) ───────────────────


def ctl_queue_counts(rabbit: dict[str, Any]) -> dict[str, tuple[int, int]]:
    """``{queue: (ready, unacked)}`` straight from the node.

    ``rabbitmqctl`` reads the queue processes directly, so unlike
    ``GET /api/queues`` it never serves a stats snapshot up to 5s old.
    """
    container = rabbit["container"]
    result = container.get_wrapped_container().exec_run(
        ["rabbitmqctl", "list_queues", "--no-table-headers", "name", "messages_ready", "messages_unacknowledged"]
    )
    output = result[1] if isinstance(result, tuple) else result.output
    if isinstance(output, bytes):
        output = output.decode("utf-8", "replace")
    counts: dict[str, tuple[int, int]] = {}
    for line in output.splitlines():
        parts = line.split("\t")
        if len(parts) != 3:
            continue  # rabbitmqctl banner lines ("Timeout: ...", "Listing queues ...")
        with contextlib.suppress(ValueError):
            counts[parts[0]] = (int(parts[1]), int(parts[2]))
    return counts


def live_counts(rabbit: dict[str, Any], queue: str, *, retries: int = 40) -> tuple[int, int]:
    """``(ready, unacked)`` for *queue*, polling until the queue exists."""
    for _ in range(retries):
        counts = ctl_queue_counts(rabbit)
        if queue in counts:
            return counts[queue]
        time.sleep(0.1)
    raise AssertionError(f"queue {queue} never appeared on the broker")


# ── passive declare probe (~0.01s; ready + consumer counts) ───────────────


class QueueProbe:
    """One pika connection reused for many passive ``queue_declare`` calls.

    A passive declare is the cheapest truthful question you can ask a live
    broker: it returns the queue's ready count and consumer count with no
    stats-collector delay. A 404 closes the *channel*, not the connection,
    so each probe takes a fresh channel.
    """

    def __init__(self, url: str) -> None:
        self._url = url
        self._connection: Any = None

    def __enter__(self) -> QueueProbe:
        return self

    def __exit__(self, *_exc: object) -> None:
        self.close()

    def close(self) -> None:
        if self._connection is not None:
            with contextlib.suppress(Exception):
                self._connection.close()
            self._connection = None

    def _channel(self) -> Any:
        import pika

        if self._connection is None or not self._connection.is_open:
            self._connection = pika.BlockingConnection(pika.URLParameters(self._url))
        return self._connection.channel()

    def declare(self, queue: str) -> Any | None:
        """``method`` of a passive declare, or None when the queue is absent."""
        try:
            channel = self._channel()
        except Exception:  # broker bounced / connection killed by a chaos test
            self._connection = None
            return None
        try:
            return channel.queue_declare(queue=queue, passive=True).method
        except Exception:
            return None
        finally:
            with contextlib.suppress(Exception):
                if channel.is_open:
                    channel.close()

    def ready(self, queue: str) -> int | None:
        method = self.declare(queue)
        return None if method is None else int(method.message_count)

    def consumers(self, queue: str) -> int | None:
        method = self.declare(queue)
        return None if method is None else int(method.consumer_count)


# ── "the consumer is actually registered" (replaces sleep(0.3)) ───────────


def queue_names_of(target: Any) -> list[str]:
    """Queue names from a broker, a single name, or an iterable of names."""
    if isinstance(target, str):
        return [target]
    routes = getattr(target, "routes", None)
    if routes is not None:
        return [route.queue.name for route in routes]
    return list(target)


def wait_for_consumers(url: str, target: Any, *, count: int = 1, timeout: float = 20.0) -> None:
    """Block until every queue of *target* has at least *count* consumer(s).

    Strictly stronger than the ``sleep(0.3)`` it replaces — that sleep was a
    guess at how long consumer registration takes; this asks the broker.
    """
    queues = queue_names_of(target)
    deadline = time.monotonic() + timeout
    with QueueProbe(url) as probe:
        pending = list(queues)
        while pending:
            pending = [q for q in pending if (probe.consumers(q) or 0) < count]
            if not pending:
                return
            if time.monotonic() >= deadline:
                raise AssertionError(f"queues never registered {count} consumer(s) within {timeout}s: {pending}")
            time.sleep(0.02)


async def await_consumers(url: str, target: Any, *, count: int = 1, timeout: float = 20.0) -> None:
    """Async form of :func:`wait_for_consumers` (runs the probe off-loop)."""
    await asyncio.to_thread(wait_for_consumers, url, target, count=count, timeout=timeout)


# ── "the broker reports these counts" (replaces management-API polling) ────


def wait_for_counts(
    rabbit: dict[str, Any],
    queue: str,
    expected: tuple[int, int],
    *,
    timeout: float = 60.0,
) -> tuple[int, int]:
    """Block until *queue* reports ``expected`` ``(ready, unacked)``.

    Two-stage on purpose: the passive declare (~10ms) gates on the ready
    count, and only when that matches do we pay for the ``rabbitmqctl`` call
    (~0.3s) that can also see unacked. Returns the last full reading so the
    caller can put it in an assertion message.
    """
    deadline = time.monotonic() + timeout
    seen: tuple[int, int] = (-1, -1)
    with QueueProbe(rabbit["url"]) as probe:
        while True:
            if probe.ready(queue) == expected[0]:
                seen = live_counts(rabbit, queue)
                if seen == expected:
                    return seen
            if time.monotonic() >= deadline:
                break
            time.sleep(0.05)
    with contextlib.suppress(AssertionError):
        seen = live_counts(rabbit, queue, retries=1)
    return seen


async def await_counts(
    rabbit: dict[str, Any],
    queue: str,
    expected: tuple[int, int],
    *,
    timeout: float = 60.0,
) -> None:
    """Async form of :func:`wait_for_counts`; raises when it never converges."""
    seen = await asyncio.to_thread(wait_for_counts, rabbit, queue, expected, timeout=timeout)
    if seen != expected:
        raise AssertionError(f"{queue}: expected ready/unacked {expected}, last saw {seen}")
