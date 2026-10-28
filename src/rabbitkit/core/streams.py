"""RabbitMQ streams consumed over AMQP 0.9.1: offsets, offset stores, tracking.

Transport-free (hard invariant 1). The brokers wire this in; see
``docs/streams.md`` for the user-facing contract.

What RabbitMQ does, verified on 3.13 and 4.1 with only the default plugins
(the ``rabbitmq_stream`` plugin is NOT needed for AMQP 0.9.1 consumers):

* A stream consumer must set a prefetch (QoS) and use manual acks; otherwise
  the broker closes the channel (406) or the connection (540).
* ``x-stream-offset`` on ``basic.consume`` picks the start: ``"first"``,
  ``"last"``, ``"next"`` (the default), a numeric offset, or an AMQP
  *timestamp* (a ``datetime``; an integer is read as an offset, not a time).
* Every delivery carries its offset in the ``x-stream-offset`` header, set by
  the broker (a publisher-supplied header of that name is overwritten).
* ``basic.nack`` / ``basic.reject`` do not redeliver and do not dead-letter:
  the message stays in the stream, and the consumer moves on.
* AMQP 0.9.1 has no server-side offset storage; a consumer that reconnects
  with the same ``x-stream-offset`` starts over from that spec.

So rabbitkit tracks the position on the client: :class:`StreamOffsetTracker`
records each delivery's offset, advances a *processed* watermark once every
earlier delivery is settled, keeps the consume arguments pointing at
``processed + 1`` (a reconnect resumes there, never at ``"next"``), and
commits the watermark to an :class:`OffsetStore` every ``commit_every``
messages or ``commit_interval`` seconds, and on ``stop()``. On restart the
consumer resumes at ``stored + 1``. A crash replays at most what was settled
since the last commit (at-least-once).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import threading
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, Protocol, TypeAlias, runtime_checkable

from rabbitkit.core.types import StreamOffsetType, validate_amqp_shortstr

if TYPE_CHECKING:
    from rabbitkit.core.message import RabbitMessage

logger = logging.getLogger(__name__)

#: Consume argument naming the start position, and the delivery header the
#: broker stamps with each message's offset.
STREAM_OFFSET_ARG = "x-stream-offset"
STREAM_CONSUMER_NAME_ARG = "x-stream-consumer-name"

#: Before the first delivery a ``"next"``/``"last"`` consumer knows no offset.
#: A reconnect in that window resubscribes from the time it first subscribed,
#: minus this margin for client/broker clock skew (a few already-retained
#: messages may be delivered; none published after subscribing is skipped).
RESUBSCRIBE_CLOCK_MARGIN = timedelta(seconds=5)


@dataclass(frozen=True, slots=True)
class StreamOffset:
    """Where a stream consumer starts when no stored offset exists.

    Use the class methods, or :meth:`parse`::

        StreamOffset.first()
        StreamOffset.last()
        StreamOffset.next()
        StreamOffset.offset(42)
        StreamOffset.timestamp(datetime(2026, 1, 1, tzinfo=UTC))
        StreamOffset.parse("first")  # also an int or a datetime

    A naive ``datetime`` is taken as UTC.
    """

    type: StreamOffsetType = StreamOffsetType.NEXT
    value: int | datetime | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.type, StreamOffsetType):
            try:
                object.__setattr__(self, "type", StreamOffsetType(self.type))
            except ValueError:
                raise ValueError(
                    f"Unknown stream offset type {self.type!r}; expected one of {[t.value for t in StreamOffsetType]}"
                ) from None
        if self.type is StreamOffsetType.OFFSET:
            if isinstance(self.value, bool) or not isinstance(self.value, int):
                raise TypeError(f"An OFFSET stream offset needs an int value, got {self.value!r}")
            if self.value < 0:
                raise ValueError("Stream offset must be non-negative")
        elif self.type is StreamOffsetType.TIMESTAMP:
            if not isinstance(self.value, datetime):
                raise TypeError(f"A TIMESTAMP stream offset needs a datetime value, got {self.value!r}")
            if self.value.tzinfo is None:
                object.__setattr__(self, "value", self.value.replace(tzinfo=UTC))
        elif self.value is not None:
            raise ValueError(f"A {self.type.value.upper()} stream offset takes no value, got {self.value!r}")

    @classmethod
    def first(cls) -> StreamOffset:
        """Start from the oldest message the stream still retains."""
        return cls(type=StreamOffsetType.FIRST)

    @classmethod
    def last(cls) -> StreamOffset:
        """Start from the last chunk written (the most recent messages)."""
        return cls(type=StreamOffsetType.LAST)

    @classmethod
    def next(cls) -> StreamOffset:
        """Only messages published after subscribing (the broker default)."""
        return cls(type=StreamOffsetType.NEXT)

    @classmethod
    def offset(cls, value: int) -> StreamOffset:
        """Start from a numeric offset."""
        return cls(type=StreamOffsetType.OFFSET, value=value)

    @classmethod
    def timestamp(cls, value: datetime) -> StreamOffset:
        """Start from the first chunk published at or after *value* (naive = UTC).

        Chunk granularity: a few messages published just before *value* may
        be delivered too.
        """
        return cls(type=StreamOffsetType.TIMESTAMP, value=value)

    @classmethod
    def parse(cls, value: StreamOffset | str | int | datetime) -> StreamOffset:
        """``"first"`` / ``"last"`` / ``"next"``, an ``int`` offset, a ``datetime``,
        or a :class:`StreamOffset` (returned as is)."""
        if isinstance(value, StreamOffset):
            return value
        if isinstance(value, bool):
            raise TypeError(f"Not a stream offset: {value!r}")
        if isinstance(value, int):
            return cls.offset(value)
        if isinstance(value, datetime):
            return cls.timestamp(value)
        if isinstance(value, str):
            relative = {"first": cls.first, "last": cls.last, "next": cls.next}
            factory = relative.get(str(value.value if isinstance(value, StreamOffsetType) else value))
            if factory is None:
                raise ValueError(
                    f"Unknown stream offset {value!r}; expected 'first', 'last', 'next', an int offset or a datetime"
                )
            return factory()
        raise TypeError(f"Not a stream offset: {value!r}")

    def to_consume_arguments(self) -> dict[str, Any]:
        """The ``basic.consume`` arguments for this start position.

        A timestamp is passed as a ``datetime``: both pika and aio-pika encode
        it as an AMQP timestamp, which RabbitMQ reads as a time. (An integer
        would be read as a numeric offset.)
        """
        if self.type is StreamOffsetType.OFFSET or self.type is StreamOffsetType.TIMESTAMP:
            return {STREAM_OFFSET_ARG: self.value}
        return {STREAM_OFFSET_ARG: self.type.value}

    def __repr__(self) -> str:
        if self.value is not None:
            return f"StreamOffset({self.type.value}={self.value})"
        return f"StreamOffset({self.type.value})"


#: Anything ``StreamOffset.parse`` accepts.
StreamOffsetLike: TypeAlias = StreamOffset | StreamOffsetType | str | int | datetime


# ── Offset stores ─────────────────────────────────────────────────────────


@runtime_checkable
class OffsetStore(Protocol):
    """Where a stream consumer's processed offset is committed.

    ``key`` identifies the consumer (``StreamConsumerConfig.consumer_name``,
    else the queue name). ``save`` stores the last *processed* offset; the
    consumer resumes at ``load(key) + 1``.

    rabbitkit ships only :class:`InMemoryOffsetStore` (offsets survive
    reconnects, not restarts). For restarts, implement these two methods over
    the storage your service already uses (its database, a key-value store, a
    file), ideally in the same transaction as the handler's own writes.

    Called from whichever thread settles messages (SyncBroker: the I/O or a
    worker thread) or from a worker thread (AsyncBroker offloads every
    store except :class:`InMemoryOffsetStore`), so implementations must be
    thread-safe. ``save`` is only called with increasing offsets per key
    within one process.
    """

    def load(self, key: str) -> int | None:
        """The committed offset for *key*, or None when nothing is stored."""
        ...

    def save(self, key: str, offset: int) -> None:
        """Commit *offset* as the last processed offset for *key*."""
        ...


class InMemoryOffsetStore:
    """Offsets kept in process memory: survive reconnects, not restarts.

    The default when a stream route sets no ``offset_store``.
    """

    def __init__(self) -> None:
        self._offsets: dict[str, int] = {}
        self._lock = threading.Lock()

    def load(self, key: str) -> int | None:
        with self._lock:
            return self._offsets.get(key)

    def save(self, key: str, offset: int) -> None:
        with self._lock:
            self._offsets[key] = offset

    def reset(self, key: str) -> None:
        """Forget *key*: its consumer starts at its configured offset again."""
        with self._lock:
            self._offsets.pop(key, None)


# ── Consumer configuration ────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class StreamConsumerConfig:
    """How a route consumes a stream queue (``subscriber(stream=...)``).

    Attributes:
        offset: where to start when *offset_store* holds nothing for this
            consumer (``StreamOffset``, or ``"first"`` / ``"last"`` /
            ``"next"`` / an int / a datetime). Default: ``"next"``.
        consumer_name: the consumer's identity: the key its offset is stored
            under (default: the queue name) and the ``x-stream-consumer-name``
            consume argument. Set it when two routes or services consume the
            same stream through one store.
        offset_store: where the processed offset is committed. ``None``: an
            :class:`InMemoryOffsetStore` (reconnects resume, restarts start at
            *offset* again). Pass your own :class:`OffsetStore`, backed by
            your service's storage, to resume across restarts.
        commit_every: commit after this many newly processed messages.
        commit_interval: also commit when this many seconds passed since the
            last commit (checked when a message is settled; ``None``: count
            only). ``stop()`` always commits. A crash replays at most the
            messages processed since the last commit.
    """

    offset: StreamOffset = field(default_factory=StreamOffset.next)
    consumer_name: str | None = None
    offset_store: OffsetStore | None = None
    commit_every: int = 100
    commit_interval: float | None = 5.0

    def __post_init__(self) -> None:
        if not isinstance(self.offset, StreamOffset):
            object.__setattr__(self, "offset", StreamOffset.parse(self.offset))
        if isinstance(self.commit_every, bool) or not isinstance(self.commit_every, int) or self.commit_every < 1:
            raise ValueError(f"StreamConsumerConfig.commit_every must be an int >= 1, got {self.commit_every!r}")
        if self.commit_interval is not None and not self.commit_interval > 0:
            raise ValueError(
                f"StreamConsumerConfig.commit_interval must be None or > 0 seconds, got {self.commit_interval!r}"
            )
        if self.consumer_name is not None:
            if not self.consumer_name:
                raise ValueError("StreamConsumerConfig.consumer_name must not be empty")
            validate_amqp_shortstr("StreamConsumerConfig.consumer_name", self.consumer_name)
        if self.offset_store is not None and not isinstance(self.offset_store, OffsetStore):
            raise TypeError(
                f"StreamConsumerConfig.offset_store must implement load(key) and save(key, offset), "
                f"got {type(self.offset_store).__name__}"
            )

    def to_consume_arguments(self) -> dict[str, Any]:
        """The ``basic.consume`` arguments for a first subscription."""
        args = self.offset.to_consume_arguments()
        if self.consumer_name is not None:
            args[STREAM_CONSUMER_NAME_ARG] = self.consumer_name
        return args

    def offset_key(self, queue: str) -> str:
        """The key this consumer's offset is stored under."""
        return self.consumer_name or queue


# ── Offset tracking ───────────────────────────────────────────────────────


class StreamOffsetTracker:
    """Tracks one stream route's position; owned by the broker (see module doc).

    The broker calls :meth:`track` on every delivery before processing it,
    passes :attr:`consume_arguments` (a live dict: a reconnect that
    re-subscribes from it resumes at the processed position) to
    ``basic.consume``, :meth:`subscribed` after each subscription,
    :meth:`begin_shutdown` when ``stop()`` starts and :meth:`flush` /
    :meth:`flush_async` once the handlers drained.

    The processed watermark advances over a contiguous prefix of settled
    deliveries, so concurrent handlers finishing out of order never move it
    past a message still in progress. An ack advances it. A nack or reject
    advances it too, because a stream never redelivers (the failure is
    logged with its offset); except after :meth:`begin_shutdown`, where a
    nack/reject is a delivery handed back unprocessed, so it is replayed on
    the next start.
    """

    def __init__(
        self,
        *,
        route_name: str,
        queue: str,
        config: StreamConsumerConfig,
        clock: Callable[[], float] = time.monotonic,
        wall_clock: Callable[[], datetime] = lambda: datetime.now(UTC),
    ) -> None:
        self._route_name = route_name
        self._config = config
        self._store: OffsetStore = config.offset_store if config.offset_store is not None else InMemoryOffsetStore()
        self._offload = not isinstance(self._store, InMemoryOffsetStore)
        self._key = config.offset_key(queue)
        self._clock = clock
        self._wall_clock = wall_clock
        self._lock = threading.Lock()
        self._commit_lock = threading.Lock()
        stored = self._store.load(self._key)
        self._processed: int | None = stored
        self._committed: int | None = stored
        #: delivered offset -> settled, in delivery (= offset) order.
        self._pending: dict[int, bool] = {}
        self._last_tag: int | None = None
        self._last_offset: int | None = None
        self._generation = 0
        self._since_commit = 0
        self._last_commit_at = clock()
        self._closing = False
        self._subscribed_at: datetime | None = None
        self._commit_task: asyncio.Task[None] | None = None
        self._arguments: dict[str, Any] = config.to_consume_arguments()
        if stored is not None:
            self._arguments[STREAM_OFFSET_ARG] = stored + 1
            logger.info(
                "Stream route %r resumes at offset %d (committed offset %d for key %r)",
                route_name,
                stored + 1,
                stored,
                self._key,
            )
        #: Store commits that raised (the next due commit retries).
        self.commit_failures = 0
        #: Deliveries settled with nack/reject while running: skipped, not retried.
        self.failed = 0

    # ── state ──────────────────────────────────────────────────────────────

    @property
    def key(self) -> str:
        """The key the offset is stored under."""
        return self._key

    @property
    def store(self) -> OffsetStore:
        """The offset store in use."""
        return self._store

    @property
    def consume_arguments(self) -> dict[str, Any]:
        """The live ``basic.consume`` arguments (updated in place)."""
        return self._arguments

    @property
    def processed_offset(self) -> int | None:
        """Highest offset with every earlier delivery settled (None: none yet)."""
        return self._processed

    @property
    def committed_offset(self) -> int | None:
        """Highest offset saved to the store (None: nothing saved)."""
        return self._committed

    # ── lifecycle ──────────────────────────────────────────────────────────

    def subscribed(self) -> None:
        """A ``basic.consume`` succeeded.

        A ``"next"``/``"last"`` consumer with nothing processed yet has no
        offset to resume from; from now on a reconnect resubscribes from the
        time of this first subscription (minus ``RESUBSCRIBE_CLOCK_MARGIN``)
        instead of ``"next"``, which would skip what was published while the
        connection was down.
        """
        with self._lock:
            self._closing = False
            if self._subscribed_at is None:
                self._subscribed_at = self._wall_clock()
            if self._processed is None and self._config.offset.type in (StreamOffsetType.NEXT, StreamOffsetType.LAST):
                self._arguments[STREAM_OFFSET_ARG] = self._subscribed_at - RESUBSCRIBE_CLOCK_MARGIN

    def begin_shutdown(self) -> None:
        """From now on a nack/reject hands the delivery back: no advance."""
        with self._lock:
            self._closing = True

    # ── deliveries ─────────────────────────────────────────────────────────

    def track(self, message: RabbitMessage) -> None:
        """Record a delivery and hook its settlement. No-op without an offset."""
        offset = message.stream_offset
        if offset is None:
            return
        tag = message.delivery_tag
        with self._lock:
            # A delivery tag or offset that does not increase means a new
            # subscription (new channel after a reconnect, or a re-subscribe):
            # deliveries still pending from the old one can never be settled
            # (their channel is gone) and are delivered again from the resume
            # point, so forget them.
            restarted = (tag is not None and self._last_tag is not None and tag <= self._last_tag) or (
                self._last_offset is not None and offset <= self._last_offset
            )
            if restarted:
                self._generation += 1
                self._pending.clear()
            self._last_tag = tag
            self._last_offset = offset
            if self._processed is not None and offset <= self._processed:
                return  # already processed (chunk-granular start): nothing to advance
            self._pending[offset] = False
            generation = self._generation
        self._hook_settlement(message, offset, generation)

    def _hook_settlement(self, message: RabbitMessage, offset: int, generation: int) -> None:
        """Wrap the transport's settlement functions: advance after a frame was sent."""
        sync_fns: list[tuple[str, Callable[..., None], bool]] = []
        if message._ack_fn is not None:
            sync_fns.append(("_ack_fn", message._ack_fn, False))
        if message._nack_fn is not None:
            sync_fns.append(("_nack_fn", message._nack_fn, True))
        if message._reject_fn is not None:
            sync_fns.append(("_reject_fn", message._reject_fn, True))
        for attr, fn, failed in sync_fns:
            setattr(message, attr, self._sync_wrapper(fn, offset, generation, failed=failed))

        async_fns: list[tuple[str, Callable[..., Awaitable[None]], bool]] = []
        if message._ack_async_fn is not None:
            async_fns.append(("_ack_async_fn", message._ack_async_fn, False))
        if message._nack_async_fn is not None:
            async_fns.append(("_nack_async_fn", message._nack_async_fn, True))
        if message._reject_async_fn is not None:
            async_fns.append(("_reject_async_fn", message._reject_async_fn, True))
        for attr, afn, failed in async_fns:
            setattr(message, attr, self._async_wrapper(afn, offset, generation, failed=failed))

    def _sync_wrapper(
        self, fn: Callable[..., None], offset: int, generation: int, *, failed: bool
    ) -> Callable[..., None]:
        def settle(*args: Any) -> None:
            fn(*args)  # raises: the frame was not sent, nothing advances
            if self._settled(offset, generation, failed=failed):
                self.commit()

        return settle

    def _async_wrapper(
        self, fn: Callable[..., Awaitable[None]], offset: int, generation: int, *, failed: bool
    ) -> Callable[..., Awaitable[None]]:
        async def settle(*args: Any) -> None:
            await fn(*args)
            if self._settled(offset, generation, failed=failed):
                self._commit_soon()

        return settle

    def _settled(self, offset: int, generation: int, *, failed: bool) -> bool:
        """Mark *offset* settled; advance the watermark. True when a commit is due."""
        with self._lock:
            if generation != self._generation or offset not in self._pending:
                return False  # from a previous subscription: delivered again
            if failed and self._closing:
                return False  # handed back at shutdown: replay it on the next start
            self._pending[offset] = True
            if failed:
                self.failed += 1
            advanced = 0
            while self._pending:
                first = next(iter(self._pending))
                if not self._pending[first]:
                    break
                del self._pending[first]
                self._processed = first
                advanced += 1
            if advanced:
                assert self._processed is not None
                self._arguments[STREAM_OFFSET_ARG] = self._processed + 1
                self._since_commit += advanced
            due = advanced > 0 and (
                self._since_commit >= self._config.commit_every
                or (
                    self._config.commit_interval is not None
                    and self._clock() - self._last_commit_at >= self._config.commit_interval
                )
            )
        if failed:
            logger.warning(
                "Stream route %r: the message at offset %d failed and is not retried (a stream does not "
                "redeliver or dead-letter); it stays in the stream and can be replayed from that offset",
                self._route_name,
                offset,
            )
        return due

    # ── commits ────────────────────────────────────────────────────────────

    def commit(self) -> None:
        """Save the processed watermark if it moved. Blocking; thread-safe."""
        with self._commit_lock:
            with self._lock:
                target = self._processed
                if target is None or (self._committed is not None and target <= self._committed):
                    return
            try:
                self._store.save(self._key, target)
            except Exception:
                # The next due commit retries; until then a crash replays more.
                self.commit_failures += 1
                logger.exception("Stream route %r: committing offset %d failed", self._route_name, target)
                return
            with self._lock:
                self._committed = target
                self._since_commit = 0
                self._last_commit_at = self._clock()

    def _commit_soon(self) -> None:
        """Async path: commit without holding up the settlement."""
        if not self._offload:
            self.commit()
            return
        task = self._commit_task
        if task is not None and not task.done():
            return  # the next due settlement commits again
        self._commit_task = asyncio.get_running_loop().create_task(
            asyncio.to_thread(self.commit), name=f"rabbitkit-stream-commit:{self._route_name}"
        )

    def flush(self) -> None:
        """Commit the processed watermark now (``stop()``)."""
        self.commit()

    async def flush_async(self) -> None:
        """Async :meth:`flush`: waits for a running commit, then commits off-loop."""
        task = self._commit_task
        if task is not None:
            with contextlib.suppress(Exception):
                await task
            self._commit_task = None
        if self._offload:
            await asyncio.to_thread(self.commit)
        else:
            self.commit()


__all__ = [
    "RESUBSCRIBE_CLOCK_MARGIN",
    "STREAM_CONSUMER_NAME_ARG",
    "STREAM_OFFSET_ARG",
    "InMemoryOffsetStore",
    "OffsetStore",
    "StreamConsumerConfig",
    "StreamOffset",
    "StreamOffsetLike",
    "StreamOffsetTracker",
    "StreamOffsetType",
]
