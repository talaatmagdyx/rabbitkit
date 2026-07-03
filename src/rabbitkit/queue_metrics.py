"""QueueMetricsPoller — bridge the RabbitMQ management API into metrics (H5).

The consume/publish counters (``MetricsMiddleware``) can only see messages
*this process* handles — they go on reading healthy while a queue silently
accumulates millions of messages because a consumer fell behind or died.
Queue depth / consumer lag / DLQ growth is the #1 RabbitMQ incident signal,
and it lives on the broker, not in-process.

``QueueMetricsPoller`` periodically calls a management client's
``list_queues()`` and emits gauges (labeled by queue) through the same
``MetricsCollector`` the rest of rabbitkit uses:

- ``{ns}_queue_messages_ready``   — backlog depth
- ``{ns}_queue_messages_unacked`` — delivered-but-unacked
- ``{ns}_queue_messages_total``   — ready + unacked
- ``{ns}_queue_consumers``        — 0 means nothing is draining

Usage (sync)::

    from rabbitkit import RabbitManagementClient, PrometheusCollector, QueueMetricsPoller

    poller = QueueMetricsPoller(
        management_client=RabbitManagementClient(...),
        collector=PrometheusCollector(),
        interval=15.0,
    )
    poller.start()   # background daemon thread
    ...
    poller.stop()

Async brokers use ``QueueMetricsPoller.start_async()`` with a management
client exposing ``list_queues_async()``.

Alert on ``queue_messages_ready`` growth and ``queue_consumers == 0`` — those
are the signals rabbitkit's own metrics cannot provide.
"""

from __future__ import annotations

import asyncio
import logging
import threading
from collections.abc import Callable
from typing import Any

from rabbitkit.core.config import MetricsConfig

logger = logging.getLogger(__name__)


class QueueMetricsPoller:
    """Polls the management API and emits queue-depth gauges.

    Args:
        management_client: object with ``list_queues(vhost)`` (sync) and/or
            ``list_queues_async(vhost)`` (async) returning a list of dicts
            with ``name``/``messages``/``messages_ready``/
            ``messages_unacknowledged``/``consumers`` keys (rabbitkit's
            ``RabbitManagementClient`` satisfies this).
        collector: any ``MetricsCollector`` (needs ``set_gauge``).
        config: metric-naming config (defaults to ``MetricsConfig()``).
        vhost: vhost to poll (default ``"/"``).
        interval: seconds between polls in the background loop.
        queue_filter: optional predicate ``(queue_name) -> bool`` — only
            matching queues emit gauges (e.g. skip rabbitkit's own delay
            queues, or restrict to a service's queues to bound cardinality).
    """

    def __init__(
        self,
        management_client: Any,
        collector: Any,
        config: MetricsConfig | None = None,
        *,
        vhost: str = "/",
        interval: float = 15.0,
        queue_filter: Callable[[str], bool] | None = None,
    ) -> None:
        self._mgmt = management_client
        self._collector = collector
        self._cfg = config or MetricsConfig()
        self._vhost = vhost
        self._interval = interval
        self._queue_filter = queue_filter
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    # ── Core: one poll → gauges ───────────────────────────────────────────

    def _emit(self, queues: list[dict[str, Any]]) -> int:
        """Set gauges for each (filtered) queue. Returns count emitted."""
        emitted = 0
        for q in queues:
            name = q.get("name")
            if not name:
                continue
            if self._queue_filter is not None and not self._queue_filter(name):
                continue
            labels = {"queue": name}
            # The management API omits counts for a queue mid-declaration or
            # in some states; default missing values to 0 rather than skip so
            # a gauge reset (queue drained) is still visible.
            ready = int(q.get("messages_ready", 0) or 0)
            unacked = int(q.get("messages_unacknowledged", 0) or 0)
            total = int(q.get("messages", ready + unacked) or 0)
            consumers = int(q.get("consumers", 0) or 0)
            self._collector.set_gauge(self._cfg.queue_messages_ready, labels, ready)
            self._collector.set_gauge(self._cfg.queue_messages_unacked, labels, unacked)
            self._collector.set_gauge(self._cfg.queue_messages_total, labels, total)
            self._collector.set_gauge(self._cfg.queue_consumers, labels, consumers)
            emitted += 1
        return emitted

    def poll_once(self) -> int:
        """Fetch queues once (sync) and emit gauges. Returns count emitted.

        Never raises — a management-API error is logged and the poll is
        skipped, so a transient management-plane outage does not crash the
        poller thread (the next tick retries).
        """
        try:
            queues = self._mgmt.list_queues(self._vhost)
        except Exception:
            logger.warning("QueueMetricsPoller: list_queues failed; skipping this poll", exc_info=True)
            return 0
        return self._emit(queues)

    async def poll_once_async(self) -> int:
        """Async variant of :meth:`poll_once`."""
        try:
            queues = await self._mgmt.list_queues_async(self._vhost)
        except Exception:
            logger.warning("QueueMetricsPoller: list_queues_async failed; skipping this poll", exc_info=True)
            return 0
        return self._emit(queues)

    # ── Background loops ──────────────────────────────────────────────────

    def start(self) -> None:
        """Start a background daemon thread polling every ``interval`` seconds."""
        if self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._run_loop, name="rabbitkit-queue-metrics", daemon=True
        )
        self._thread.start()

    def _run_loop(self) -> None:
        # Poll immediately, then every interval. Event.wait doubles as the
        # sleep and the stop signal (interruptible shutdown).
        while not self._stop.is_set():
            self.poll_once()
            self._stop.wait(self._interval)

    def stop(self, timeout: float = 5.0) -> None:
        """Signal the background thread to stop and join it."""
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=timeout)
            self._thread = None

    async def run_async(self) -> None:
        """Async polling loop — run as a task; cancel to stop.

        Usage::

            task = asyncio.create_task(poller.run_async())
            ...
            task.cancel()
        """
        while True:
            await self.poll_once_async()
            await asyncio.sleep(self._interval)
