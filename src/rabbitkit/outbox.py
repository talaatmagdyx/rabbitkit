"""Producer outbox relays: ``AsyncOutbox`` and ``SyncOutbox``.

Both are thin over one :class:`~rabbitkit.core.outbox.OutboxStore` (SQLite):
the application enqueues an envelope durably, and the relay publishes it
through the broker's normal ``publish()`` and deletes it only after
``CONFIRMED``. See ``docs/outbox.md`` for the guarantees and limits.

``AsyncOutbox`` runs the relay as one task on the broker's event loop and
every SQLite call in ``asyncio.to_thread``. ``SyncOutbox`` owns no thread:
``SyncBroker`` may only be used by the thread that started it, so the relay
runs on the thread that calls :meth:`SyncOutbox.relay_once` /
:meth:`SyncOutbox.run`, which must be the broker's owner thread.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import os
import random
import threading
import time
from collections.abc import Callable, Iterable, Sequence
from types import TracebackType
from typing import TYPE_CHECKING, Protocol

from rabbitkit.core.config import MetricsConfig, OutboxConfig
from rabbitkit.core.errors import ConfigValidationError, OutboxClosedError
from rabbitkit.core.outbox import (
    OutboxEntry,
    OutboxPassReport,
    OutboxStats,
    OutboxStore,
    is_transient_result,
    outbox_backoff,
)
from rabbitkit.core.types import MessageEnvelope, OutboxEntryState, PublishOutcome

if TYPE_CHECKING:
    from rabbitkit.middleware.metrics import MetricsCollector

__all__ = ["AsyncOutbox", "AsyncOutboxPublisher", "SyncOutbox", "SyncOutboxPublisher"]

logger = logging.getLogger("rabbitkit.outbox")


class AsyncOutboxPublisher(Protocol):
    """What ``AsyncOutbox`` needs from a broker (``AsyncBroker`` satisfies it)."""

    async def publish(
        self, envelope: MessageEnvelope | None = None, *, timeout: float | None = None
    ) -> PublishOutcome: ...


class SyncOutboxPublisher(Protocol):
    """What ``SyncOutbox`` needs from a broker (``SyncBroker`` satisfies it)."""

    def publish(self, envelope: MessageEnvelope | None = None) -> PublishOutcome: ...


def _require_confirms(broker: object) -> None:
    """The relay deletes an entry only on a broker confirm; without confirms
    every publish is ``SENT`` and nothing could ever be relayed."""
    publisher = getattr(getattr(broker, "config", None), "publisher", None)
    if publisher is not None and getattr(publisher, "confirm_delivery", True) is False:
        raise ConfigValidationError(
            "The outbox needs publisher confirms: it deletes an entry only after the broker confirms it. "
            "Set PublisherConfig(confirm_delivery=True) on the broker the outbox publishes through."
        )


def _metrics_of(
    broker: object, collector: MetricsCollector | None, metrics_config: MetricsConfig | None
) -> tuple[MetricsCollector | None, MetricsConfig | None]:
    """An explicit collector wins; else the broker's first ``MetricsMiddleware``'s."""
    if collector is not None:
        return collector, metrics_config
    finder = getattr(broker, "_bulk_metrics", None)
    found = finder() if callable(finder) else None
    if found is None:
        return None, metrics_config
    return found[0], metrics_config or found[1]


class AsyncOutbox:
    """Durable producer outbox for ``AsyncBroker``.

    Args:
        broker: Publishes the entries (``await broker.publish(envelope)``).
            The outbox does not start or stop it.
        path: SQLite file of the store (local disk).
        config: Bounds, relay pacing, durability.
        collector: Metrics collector for the ``outbox_*`` metrics. Default:
            the collector of the broker's first ``MetricsMiddleware``.
        metrics_config: Metric names.

    Usage::

        outbox = AsyncOutbox(broker, "/var/lib/app/outbox.db")
        await outbox.start()
        await outbox.enqueue(MessageEnvelope(routing_key="orders", body=b"...", mandatory=True))
        ...
        await outbox.stop(timeout=10)   # before broker.stop()

    Everything must run on one event loop (the broker's).
    """

    def __init__(
        self,
        broker: AsyncOutboxPublisher,
        path: str | os.PathLike[str],
        config: OutboxConfig | None = None,
        *,
        collector: MetricsCollector | None = None,
        metrics_config: MetricsConfig | None = None,
    ) -> None:
        self._broker = broker
        self._config = config or OutboxConfig()
        found_collector, found_config = _metrics_of(broker, collector, metrics_config)
        self._store = OutboxStore(path, self._config, collector=found_collector, metrics_config=found_config)
        self._rng: Callable[[], float] = random.random
        self._task: asyncio.Task[bool] | None = None
        self._wake: asyncio.Event | None = None
        self._pass_lock: asyncio.Lock | None = None
        self._stopping = False
        self._closed = False

    @property
    def store(self) -> OutboxStore:
        """The underlying store (blocking calls; use from a thread)."""
        return self._store

    @property
    def config(self) -> OutboxConfig:
        return self._config

    @property
    def running(self) -> bool:
        """True while the relay task is running."""
        return self._task is not None and not self._task.done()

    # ── lifecycle ─────────────────────────────────────────────────────

    async def start(self) -> None:
        """Open the store and start the relay (idempotent).

        Every entry already pending in the file, including the ones a
        crashed process was publishing, is relayed again. Raises
        ``ConfigValidationError`` when the broker has publisher confirms off.
        """
        if self.running:
            return
        _require_confirms(self._broker)
        await asyncio.to_thread(self._store.open)
        self._closed = False
        self._stopping = False
        self._wake = asyncio.Event()
        self._pass_lock = asyncio.Lock()
        self._task = asyncio.create_task(self._run(), name="rabbitkit-outbox-relay")

    async def stop(self, timeout: float | None = None) -> bool:
        """Stop enqueueing, drain, stop the relay, close the store.

        Keeps relaying until nothing is due or *timeout* (default
        ``OutboxConfig.drain_timeout``) expires, then cancels the relay.
        Nothing is lost either way: unconfirmed entries stay in the file for
        the next start. Returns True when the relay drained (nothing due
        was left). Call it before ``broker.stop()``.
        """
        self._closed = True
        task = self._task
        drained = True
        if task is not None:
            self._stopping = True
            if self._wake is not None:
                self._wake.set()
            budget = self._config.drain_timeout if timeout is None else timeout
            done, _pending = await asyncio.wait({task}, timeout=max(0.0, budget))
            if done:
                drained = task.result()
            else:
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
                drained = False
            self._task = None
        stats = await asyncio.to_thread(self._store.stats)
        await asyncio.to_thread(self._store.close)
        logger.info(
            "Outbox stopped (%s): %d pending and %d failed entries stay in %s",
            "drained" if drained else "drain timed out",
            stats.pending,
            stats.failed,
            self._store.path,
        )
        return drained

    async def __aenter__(self) -> AsyncOutbox:
        await self.start()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        await self.stop()

    # ── enqueue ───────────────────────────────────────────────────────

    async def enqueue(self, envelope: MessageEnvelope) -> int:
        """Store *envelope* durably (off the event loop) and wake the relay.

        Returns the entry id once the write is committed. Raises
        ``OutboxFullError`` (nothing written) at a bound and
        ``OutboxClosedError`` after :meth:`stop`. A cancelled call may or may
        not have stored the entry (the write finishes in its thread).
        """
        if self._closed:
            raise OutboxClosedError("The outbox is stopped; nothing was written")
        entry_id = await asyncio.to_thread(self._store.enqueue, envelope)
        self.notify()
        return entry_id

    async def enqueue_many(self, envelopes: Sequence[MessageEnvelope]) -> list[int]:
        """Store several envelopes in one transaction (all or nothing)."""
        if self._closed:
            raise OutboxClosedError("The outbox is stopped; nothing was written")
        ids = await asyncio.to_thread(self._store.enqueue_many, list(envelopes))
        self.notify()
        return ids

    def notify(self) -> None:
        """Wake the relay now (e.g. after committing a transactional
        ``store.enqueue_in``). Call it on the outbox's event loop; from
        another thread use ``loop.call_soon_threadsafe(outbox.notify)``."""
        if self._wake is not None:
            self._wake.set()

    # ── relay ─────────────────────────────────────────────────────────

    async def _publish(self, entry: OutboxEntry) -> PublishOutcome | BaseException:
        try:
            if self._config.publish_timeout is None:
                return await self._broker.publish(entry.envelope)
            return await self._broker.publish(entry.envelope, timeout=self._config.publish_timeout)
        except Exception as exc:  # recorded against the entry, never lost
            return exc

    async def relay_once(self) -> OutboxPassReport:
        """Run one relay pass: publish up to ``max_in_flight`` due entries
        concurrently and record their outcomes in one transaction."""
        lock = self._pass_lock
        if lock is None:
            lock = self._pass_lock = asyncio.Lock()
        async with lock:
            entries = await asyncio.to_thread(self._store.due, self._config.max_in_flight)
            if not entries:
                return OutboxPassReport()
            results = await asyncio.gather(*(self._publish(entry) for entry in entries))
            return await asyncio.to_thread(self._store.settle, list(zip(entries, results, strict=True)))

    async def _idle(self) -> None:
        wake = self._wake
        assert wake is not None
        with contextlib.suppress(TimeoutError):
            await asyncio.wait_for(wake.wait(), self._config.poll_interval)
        wake.clear()

    async def _run(self) -> bool:
        consecutive = 0
        while True:
            report: OutboxPassReport | None
            try:
                report = await self.relay_once()
            except Exception:  # task boundary: a store error must not kill the relay
                logger.exception("Outbox relay pass failed; retrying after a backoff")
                report = None
            if report is not None and report.attempted == 0:
                if self._stopping:
                    return True
                await self._idle()
                continue
            if report is None or report.transient:
                consecutive += 1
                delay = outbox_backoff(self._config, consecutive, self._rng)
                logger.debug("Outbox relay backing off %.2fs (failure %d)", delay, consecutive)
                await asyncio.sleep(delay)
            elif report.confirmed:
                consecutive = 0

    # ── inspection and operator actions ───────────────────────────────

    async def stats(self) -> OutboxStats:
        return await asyncio.to_thread(self._store.stats)

    async def list_entries(
        self, state: OutboxEntryState = OutboxEntryState.FAILED, *, limit: int = 100, after_id: int = 0
    ) -> list[OutboxEntry]:
        return await asyncio.to_thread(self._store.list_entries, state, limit=limit, after_id=after_id)

    async def replay_failed(self, ids: Iterable[int] | None = None) -> int:
        """Move failed entries back to pending (see ``OutboxStore.replay_failed``)."""
        selected = None if ids is None else list(ids)
        count = await asyncio.to_thread(self._store.replay_failed, selected)
        self.notify()
        return count

    async def purge_failed(self, ids: Iterable[int] | None = None) -> int:
        """Delete failed entries (see ``OutboxStore.purge_failed``)."""
        selected = None if ids is None else list(ids)
        return await asyncio.to_thread(self._store.purge_failed, selected)


class SyncOutbox:
    """Durable producer outbox for ``SyncBroker``.

    Same store and guarantees as :class:`AsyncOutbox`, without a thread of
    its own. ``enqueue()`` is safe from any thread. The relay
    (:meth:`relay_once`, :meth:`drain`, :meth:`run`) publishes through
    ``broker.publish()`` and must run on the broker's owner thread (the
    thread that called ``broker.start()``). For a process whose owner thread
    is busy consuming, run the relay on its own thread with its own
    publish-only ``SyncBroker``, started on that thread (see
    ``docs/outbox.md``).

    Args: as :class:`AsyncOutbox`.
    """

    def __init__(
        self,
        broker: SyncOutboxPublisher,
        path: str | os.PathLike[str],
        config: OutboxConfig | None = None,
        *,
        collector: MetricsCollector | None = None,
        metrics_config: MetricsConfig | None = None,
    ) -> None:
        self._broker = broker
        self._config = config or OutboxConfig()
        found_collector, found_config = _metrics_of(broker, collector, metrics_config)
        self._store = OutboxStore(path, self._config, collector=found_collector, metrics_config=found_config)
        self._rng: Callable[[], float] = random.random
        self._wake = threading.Event()
        self._stop_requested = threading.Event()
        self._closed = False

    @property
    def store(self) -> OutboxStore:
        return self._store

    @property
    def config(self) -> OutboxConfig:
        return self._config

    # ── enqueue (any thread) ──────────────────────────────────────────

    def enqueue(self, envelope: MessageEnvelope) -> int:
        """Store *envelope* durably and return its entry id (any thread).

        Raises ``OutboxFullError`` (nothing written) at a bound and
        ``OutboxClosedError`` after :meth:`close`.
        """
        if self._closed:
            raise OutboxClosedError("The outbox is closed; nothing was written")
        entry_id = self._store.enqueue(envelope)
        self._wake.set()
        return entry_id

    def enqueue_many(self, envelopes: Sequence[MessageEnvelope]) -> list[int]:
        """Store several envelopes in one transaction (all or nothing)."""
        if self._closed:
            raise OutboxClosedError("The outbox is closed; nothing was written")
        ids = self._store.enqueue_many(list(envelopes))
        self._wake.set()
        return ids

    def notify(self) -> None:
        """Wake an idle :meth:`run` loop now (any thread)."""
        self._wake.set()

    # ── relay (broker owner thread) ───────────────────────────────────

    def _pass(self, deadline: float | None) -> OutboxPassReport:
        entries = self._store.due(self._config.max_in_flight)
        if not entries:
            return OutboxPassReport()
        results: list[tuple[OutboxEntry, PublishOutcome | BaseException]] = []
        for entry in entries:
            if results and deadline is not None and time.monotonic() >= deadline:
                break
            try:
                result: PublishOutcome | BaseException = self._broker.publish(entry.envelope)
            except Exception as exc:  # recorded against the entry, never lost
                result = exc
            results.append((entry, result))
            if is_transient_result(result):
                # The broker is unreachable: publishing the rest now would
                # only wait out the same failure once per entry.
                break
        return self._store.settle(results)

    def relay_once(self) -> OutboxPassReport:
        """Run one pass: publish up to ``max_in_flight`` due entries, one
        after another, and record their outcomes in one transaction. Stops
        the pass early at the first transient outcome. Owner thread only."""
        self._require_open()
        return self._pass(None)

    def drain(self, timeout: float | None = None) -> bool:
        """Relay until nothing is due or *timeout* (default
        ``OutboxConfig.drain_timeout``) passes. Owner thread only.

        The deadline is checked between publishes, so it can be overrun by
        one publish (bounded by the broker's ``confirm_timeout``). Returns
        True when nothing due is left.
        """
        self._require_open()
        budget = self._config.drain_timeout if timeout is None else timeout
        deadline = time.monotonic() + budget
        consecutive = 0
        while True:
            report = self._pass(deadline)
            if report.attempted == 0:
                return True
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return False
            if report.transient:
                consecutive += 1
                time.sleep(min(remaining, outbox_backoff(self._config, consecutive, self._rng)))
            elif report.confirmed:
                consecutive = 0

    def run(self) -> bool:
        """Relay until :meth:`request_stop`, then drain for
        ``OutboxConfig.drain_timeout``. Blocks; owner thread only.

        Returns the result of the final :meth:`drain`.
        """
        self._require_open()
        self._store.open()
        self._stop_requested.clear()
        consecutive = 0
        while not self._stop_requested.is_set():
            report: OutboxPassReport | None
            try:
                report = self._pass(None)
            except Exception:  # loop boundary: a store error must not end the relay
                logger.exception("Outbox relay pass failed; retrying after a backoff")
                report = None
            if report is not None and report.attempted == 0:
                self._wake.wait(self._config.poll_interval)
                self._wake.clear()
                continue
            if report is None or report.transient:
                consecutive += 1
                self._stop_requested.wait(outbox_backoff(self._config, consecutive, self._rng))
            elif report.confirmed:
                consecutive = 0
        return self.drain()

    def request_stop(self) -> None:
        """Make :meth:`run` drain and return (any thread, e.g. a signal handler)."""
        self._stop_requested.set()
        self._wake.set()

    def close(self) -> None:
        """Close the store. Later enqueues raise ``OutboxClosedError``;
        pending entries stay in the file for the next outbox."""
        self._closed = True
        self._store.close()

    def __enter__(self) -> SyncOutbox:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()

    def _require_open(self) -> None:
        if self._closed:
            raise OutboxClosedError("The outbox is closed")
        _require_confirms(self._broker)

    # ── inspection and operator actions ───────────────────────────────

    def stats(self) -> OutboxStats:
        return self._store.stats()

    def list_entries(
        self, state: OutboxEntryState = OutboxEntryState.FAILED, *, limit: int = 100, after_id: int = 0
    ) -> list[OutboxEntry]:
        return self._store.list_entries(state, limit=limit, after_id=after_id)

    def replay_failed(self, ids: Iterable[int] | None = None) -> int:
        """Move failed entries back to pending (see ``OutboxStore.replay_failed``)."""
        count = self._store.replay_failed(ids)
        self._wake.set()
        return count

    def purge_failed(self, ids: Iterable[int] | None = None) -> int:
        """Delete failed entries (see ``OutboxStore.purge_failed``)."""
        return self._store.purge_failed(ids)
