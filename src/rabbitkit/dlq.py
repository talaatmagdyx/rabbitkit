"""DLQ Inspector — peek, replay, and purge dead-letter queues.

Provides inspection and recovery tools for messages stuck in DLQs.

**Operational realism:**
- ``peek()`` returns materialized snapshots, not live references
- ``peek()`` may affect message ordering (basic.get + requeue changes position)
- ``replay()`` preserves original headers (pass ``reset_retry_count=True`` to
  grant the replayed message a fresh retry ladder)
- ``replay()`` acks a DLQ original only after the republish outcome is OK;
  failed republishes are nack-requeued so they stay on the DLQ
- ``purge()`` is immediate and unfiltered — use ``replay()`` for selective recovery

**Quorum queues.** AMQP 0-9-1 has no browse, so a peek is ``basic.get`` plus
a requeue, and a quorum queue counts every requeue as a delivery. Past the
queue's delivery limit (20 by default on RabbitMQ 4.x) the broker drops the
message or dead-letters it away. Pass a management client and the inspector
refuses up front to peek a quorum queue whose limit isn't unlimited.
Without one it stops as soon as a fetched message carries
``x-delivery-count``, which only a quorum queue sets. See
:mod:`rabbitkit.core.quorum`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncIterator, Callable, Iterator
from typing import Any

from rabbitkit.core.errors import UnsafeToBrowseError
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.quorum import (
    DEFAULT_MAX_QUORUM_SCAN,
    DELIVERY_COUNT_HEADER,
    assert_browsable,
    assert_whole_scan,
    is_quorum,
    too_deep,
    unlimited_fix,
)
from rabbitkit.core.types import MessageEnvelope

logger = logging.getLogger(__name__)

# Matches RetryConfig.retry_header's default. If you customized retry_header,
# strip your header via a predicate/pre-processing step instead.
_RETRY_COUNT_HEADER = "x-rabbitkit-retry-count"
_ORIGINAL_QUEUE_HEADER = "x-rabbitkit-original-queue"


def _header_str(value: Any) -> str | None:
    if isinstance(value, bytes):
        value = value.decode(errors="replace")
    return value if isinstance(value, str) and value else None


def original_queue(headers: dict[str, Any]) -> str | None:
    """The queue a dead-lettered message came from, read from its headers.

    ``x-rabbitkit-original-queue`` is only on the wire when the message went
    through a retry republish. The broker sets the rest itself when it
    dead-letters: ``x-last-death-queue`` (3.13+, the hop that put the message
    in THIS queue) and ``x-death``, whose newest entry comes first. Without
    them, replay would fall back to the routing key, which on a broker
    dead-lettered message is the DLQ's own name.
    """
    for key in (_ORIGINAL_QUEUE_HEADER, "x-last-death-queue"):
        found = _header_str(headers.get(key))
        if found:
            return found
    deaths = headers.get("x-death")
    if isinstance(deaths, list) and deaths and isinstance(deaths[0], dict):
        return _header_str(deaths[0].get("queue"))
    return None


def original_routing_key(headers: dict[str, Any]) -> str | None:
    """The routing key a dead-lettered message was published with.

    ``x-rabbitkit-original-routing-key`` when a retry republish recorded it,
    else the routing keys of the newest ``x-death`` entry. A broker
    dead-lettered message's own routing key is the DLQ's name.
    """
    found = _header_str(headers.get("x-rabbitkit-original-routing-key"))
    if found:
        return found
    deaths = headers.get("x-death")
    if isinstance(deaths, list) and deaths and isinstance(deaths[0], dict):
        keys = deaths[0].get("routing-keys")
        if isinstance(keys, list) and keys:
            return _header_str(keys[0])
    return None


def _release_messages(messages: list[RabbitMessage]) -> None:
    """Requeue whatever is still unsettled. Never raises: this runs in
    ``finally`` blocks, and the original error must win."""
    for msg in messages:
        if msg.is_settled:
            continue
        try:
            msg.nack(requeue=True)
        except Exception:
            logger.warning("Could not requeue a held DLQ message; it returns when its channel closes", exc_info=True)


async def _release_messages_async(messages: list[RabbitMessage]) -> None:
    for msg in messages:
        if msg.is_settled:
            continue
        try:
            await msg.nack_async(requeue=True)
        except Exception:
            logger.warning("Could not requeue a held DLQ message; it returns when its channel closes", exc_info=True)


def _is_not_found(exc: BaseException) -> bool:
    return 404 in (getattr(exc, "code", None), getattr(exc, "status", None))


class ReplayResult(int):
    """Replay report that IS the replayed count (int-compatible, so existing
    ``count = inspector.replay(...)`` callers keep working), with extras:

    - ``failed``: messages whose republish outcome was not OK — they were
      nack-requeued and REMAIN ON THE DLQ.
    - ``requeued``: non-matching messages returned to the DLQ (predicate
      returned False).
    - ``skipped``: messages with no safe destination — routing would have
      republished them into the queue being drained. They REMAIN ON THE DLQ;
      pass ``target_queue``.
    """

    failed: int
    requeued: int
    skipped: int

    def __new__(cls, replayed: int, failed: int = 0, requeued: int = 0, skipped: int = 0) -> ReplayResult:
        obj = super().__new__(cls, replayed)
        obj.failed = failed
        obj.requeued = requeued
        obj.skipped = skipped
        return obj

    def __repr__(self) -> str:
        return (
            f"ReplayResult(replayed={int(self)}, failed={self.failed}, requeued={self.requeued}, "
            f"skipped={self.skipped})"
        )


class DLQInspector:
    """Dead-letter queue inspection and replay.

    Accepts a transport that supports ``basic_get``, ``publish``,
    ``purge_queue`` methods. Works with both sync and async transports.
    rabbitkit's transports also provide ``inspection_session()``, and the
    inspector then runs each operation on a channel of its own.

    Usage::

        inspector = DLQInspector(transport, management=RabbitManagementClient())

        # Peek at messages without consuming them
        messages = inspector.peek("orders-queue.dlq", limit=5)

        # Replay matching messages back to source queue
        count = inspector.replay(
            "orders-queue.dlq",
            predicate=lambda msg: msg.headers.get("x-error") == "timeout",
            target_queue="orders-queue",
        )

        # Purge entire DLQ
        count = inspector.purge("orders-queue.dlq")

    Args:
        transport: A rabbitkit transport (sync or async) or anything with the
            same ``basic_get`` / ``publish`` / ``purge_queue`` methods.
        management: Optional :class:`~rabbitkit.management.RabbitManagementClient`.
            With it, ``peek`` and filtered ``replay`` check the queue's type
            and delivery limit first and raise :class:`UnsafeToBrowseError`
            for a limited quorum queue, failing closed when the limit can't
            be determined. Without it, they stop and raise as soon as a
            fetched message carries ``x-delivery-count``.
        vhost: Virtual host to look the queue up in on the management API.
            Defaults to the transport's configured vhost.
        check_delivery_limit: Set False to skip both checks, e.g. for a
            quorum DLQ you know is unlimited and have no management access to.
    """

    def __init__(
        self,
        transport: Any,
        *,
        management: Any = None,
        vhost: str | None = None,
        check_delivery_limit: bool = True,
        max_quorum_scan: int = DEFAULT_MAX_QUORUM_SCAN,
    ) -> None:
        self._transport = transport
        self._management = management
        self._max_quorum_scan = max_quorum_scan
        # Vetted quorum queues: read only whole (a partial read reorders them).
        self._whole: set[str] = set()
        if vhost is None:
            configured = getattr(getattr(transport, "_connection_config", None), "vhost", None)
            vhost = configured if isinstance(configured, str) else "/"
        self._vhost = vhost
        self._check_delivery_limit = check_delivery_limit
        self._rabbitmq_version: str | None = None
        # Queues the tripwire has seen to be quorum: refuse those up front
        # instead of spending another delivery on the head message.
        self._known_quorum: set[str] = set()

    # ── Delivery-limit guard ─────────────────────────────────────────────

    def _queue_info(self, queue: str) -> dict[str, Any] | None:
        try:
            return dict(self._management.get_queue(queue, vhost=self._vhost))
        except Exception as exc:
            if _is_not_found(exc):
                return None  # the AMQP call reports the missing queue as usual
            raise UnsafeToBrowseError(
                f"Could not read {queue!r} from the management API to check its delivery limit: {exc}"
            ) from exc

    def _version(self) -> str | None:
        if self._rabbitmq_version is None:
            try:
                overview = self._management.overview()
            except Exception as exc:
                raise UnsafeToBrowseError(
                    f"Could not read the RabbitMQ version from the management API: {exc}"
                ) from exc
            version = overview.get("rabbitmq_version")
            self._rabbitmq_version = str(version) if version else None
        return self._rabbitmq_version

    def _ensure_browsable(self, queue: str) -> bool:
        """Raise if requeueing from *queue* is unsafe; True if it was VETTED.

        False means nothing could be checked up front (no management client,
        or the management API doesn't know the queue, e.g. a wrong vhost),
        so the caller must keep the ``x-delivery-count`` tripwire armed.
        """
        if not self._check_delivery_limit:
            return True
        if queue in self._known_quorum:
            self._raise_tripwire(queue)
        if self._management is None:
            return False
        info = self._queue_info(queue)
        if info is None:
            return False
        assert_browsable(queue, info, self._version())
        if is_quorum(info):
            assert_whole_scan(queue, info, self._max_quorum_scan)
            self._whole.add(queue)
        else:
            self._whole.discard(queue)
        return True

    def _fetch_limit(self, queue: str, limit: int) -> int:
        # one past the scan limit: a whole read must see the queue's end
        return self._max_quorum_scan + 1 if queue in self._whole else limit

    def _check_whole(self, queue: str, fetched: int) -> None:
        if queue in self._whole and fetched > self._max_quorum_scan:  # grew while it was read
            raise UnsafeToBrowseError(too_deep(queue, fetched, self._max_quorum_scan))

    def _check_partial_replay(self, queue: str, predicate: Any, limit: int | None) -> None:
        if queue in self._whole and predicate is not None and limit is not None:
            raise UnsafeToBrowseError(
                f"{queue!r} is a quorum queue: a filtered replay with a limit would requeue the "
                "non-matching messages behind the ones it didn't read, which reorders the queue. "
                "Drop the limit, so the replay reads the whole queue and keeps its order."
            )

    async def _ensure_browsable_async(self, queue: str) -> bool:
        # The management client's sync methods need no aiohttp; run them off
        # the event loop.
        if self._check_delivery_limit and self._management is not None:
            return await asyncio.to_thread(self._ensure_browsable, queue)
        return self._ensure_browsable(queue)

    def _tripwire(self, queue: str, msg: RabbitMessage, vetted: bool) -> bool:
        """True if *msg* proves *queue* is a quorum queue we couldn't vet."""
        if vetted or DELIVERY_COUNT_HEADER not in msg.headers:
            return False
        self._known_quorum.add(queue)
        return True

    def _raise_tripwire(self, queue: str) -> None:
        raise UnsafeToBrowseError(
            f"{queue!r} is a quorum queue: its messages carry {DELIVERY_COUNT_HEADER!r}, and every "
            "peek or non-matching replay counts as a delivery. Its delivery limit can't be checked "
            "without the management API, so the inspector stopped. Pass "
            "DLQInspector(..., management=RabbitManagementClient(...)) to verify the limit, or make "
            f"the queue unlimited first: {unlimited_fix(None)}. Pass check_delivery_limit=False to "
            "skip this check."
        )

    def _vet_requeue(self, queue: str, msg: RabbitMessage, vetted: bool | None) -> bool:
        """An unfiltered replay is about to requeue *msg*: check now.

        Every failed or skipped message costs a delivery each run, so a
        message that keeps failing on a limited quorum DLQ would eventually
        be dropped. Raises (the ``finally`` then releases what is held).
        """
        if vetted is None:
            vetted = self._ensure_browsable(queue)
        if self._tripwire(queue, msg, vetted):
            self._raise_tripwire(queue)
        return vetted

    async def _vet_requeue_async(self, queue: str, msg: RabbitMessage, vetted: bool | None) -> bool:
        if vetted is None:
            vetted = await self._ensure_browsable_async(queue)
        if self._tripwire(queue, msg, vetted):
            self._raise_tripwire(queue)
        return vetted

    # ── Sessions ─────────────────────────────────────────────────────────

    def _session_opener(self) -> Callable[[], Any] | None:
        # Looked up on the CLASS: a MagicMock transport answers every
        # attribute, and a mocked session's basic_get never returns None.
        opener = getattr(type(self._transport), "inspection_session", None)
        if opener is None:
            return None
        transport = self._transport
        return lambda: opener(transport)

    @contextlib.contextmanager
    def _session(self) -> Iterator[Any]:
        opener = self._session_opener()
        if opener is None:
            yield self._transport
            return
        with opener() as session:
            yield session

    @contextlib.asynccontextmanager
    async def _session_async(self) -> AsyncIterator[Any]:
        opener = self._session_opener()
        if opener is None:
            yield self._transport
            return
        async with opener() as session:
            yield session

    # ── Sync methods ─────────────────────────────────────────────────────

    def peek(self, queue: str, limit: int = 10) -> list[RabbitMessage]:
        """Fetch up to ``limit`` messages from the queue, then requeue them.

        Every message is held unacked until the fetch loop ends, so the loop
        never re-fetches one it already has. All of them are requeued in a
        ``finally``, in the order they were read, including when
        ``basic_get`` fails midway.

        A quorum queue the management API vetted is read whole (up to
        ``max_quorum_scan``) and the first ``limit`` messages are returned:
        it puts returned messages at the back, so a partial read would
        reorder it. Without a management client a quorum queue can't be
        told apart until its messages carry ``x-delivery-count``, so the
        first peek of an unvetted one can still reorder it.

        Raises:
            UnsafeToBrowseError: The queue is a quorum queue with a delivery
                limit, or one whose limit can't be verified (see the class
                docstring).

        Returns a list of message snapshots.
        """
        vetted = self._ensure_browsable(queue)
        messages: list[RabbitMessage] = []
        with self._session() as session:
            try:
                for _ in range(self._fetch_limit(queue, limit)):
                    msg = session.basic_get(queue)
                    if msg is None:
                        break
                    messages.append(msg)
                    if self._tripwire(queue, msg, vetted):
                        self._raise_tripwire(queue)
                self._check_whole(queue, len(messages))
            finally:
                _release_messages(messages)
        return messages[:limit]

    @staticmethod
    def _resolve_routing_key(msg: RabbitMessage, target_queue: str | None) -> str:
        return target_queue or original_queue(msg.headers) or msg.routing_key

    @staticmethod
    def _build_replay_envelope(
        msg: RabbitMessage,
        target_queue: str | None,
        target_exchange: str | None,
        reset_retry_count: bool,
    ) -> MessageEnvelope:
        """Build the republish envelope for one DLQ message.

        ``mandatory=True`` so an unroutable target comes back as a
        ``RETURNED`` outcome instead of being broker-confirmed into the void.

        Every property the original carried is copied, and a property it
        lacked stays absent: an empty ``message_id`` / ``content_type`` is
        sent as no property at all. Two caveats: aiormq gives every publish
        without a ``message_id`` a random one (it needs one to match
        returns), and an unknown ``delivery_mode`` is sent as 2
        (persistent), so replay never makes a message less durable.
        """
        headers = dict(msg.headers)
        if reset_retry_count:
            headers.pop(_RETRY_COUNT_HEADER, None)
        return MessageEnvelope(
            routing_key=DLQInspector._resolve_routing_key(msg, target_queue),
            body=msg.body,
            exchange=target_exchange if target_exchange is not None else "",
            headers=headers,
            message_id=msg.message_id or "",
            correlation_id=msg.correlation_id,
            content_type=msg.content_type or "",
            content_encoding=msg.content_encoding,
            # Preserve the remaining original message properties -- these used
            # to be silently dropped on replay, e.g. a priority-queue message
            # lost its priority, and an RPC request's reply_to/type/app_id/
            # user_id never survived the replay for the reply to route back.
            reply_to=msg.reply_to,
            priority=msg.priority,
            expiration=msg.expiration,
            timestamp=msg.timestamp,
            # Unknown (a hand-built message, a duck-typed transport) stays
            # persistent, as replay always was; a known 1 stays transient.
            delivery_mode=msg.delivery_mode if msg.delivery_mode in (1, 2) else 2,
            type=msg.type,
            app_id=msg.app_id,
            user_id=msg.user_id,
            mandatory=True,
        )

    @staticmethod
    def _is_self_replay(queue: str, envelope: MessageEnvelope, allow_self_replay: bool) -> bool:
        if allow_self_replay or envelope.exchange or envelope.routing_key != queue:
            return False
        logger.error(
            "DLQ replay skipped: no original queue on the message, so it would be republished into "
            "%r itself (message_id=%s). Pass target_queue to replay it.",
            queue,
            envelope.message_id,
        )
        return True

    def replay(
        self,
        queue: str,
        predicate: Callable[[RabbitMessage], bool] | None = None,
        target_queue: str | None = None,
        target_exchange: str | None = None,
        *,
        reset_retry_count: bool = False,
        limit: int | None = None,
        allow_self_replay: bool = False,
    ) -> ReplayResult:
        """Replay messages from the DLQ.

        Fetches messages, applies optional predicate filter, publishes
        matching messages to the target, and acks each original **only after
        its republish outcome is OK**. A failed republish (NACKED / TIMEOUT /
        RETURNED / ERROR) is nack-requeued, so the message stays on the DLQ
        instead of being lost.

        Non-matching messages are nacked with ``requeue=True``.

        Args:
            queue: Source DLQ to replay from.
            predicate: Optional filter — only replay messages where
                predicate returns True. All messages replayed if None.
            target_queue: Target queue routing key. Defaults to the queue
                the message was dead-lettered from: the
                ``x-rabbitkit-original-queue`` header, then the broker's
                ``x-last-death-queue`` / ``x-death``, then the routing key.
            target_exchange: Target exchange. Defaults to "".
            reset_retry_count: Strip the ``x-rabbitkit-retry-count`` header
                so the replayed message gets a fresh retry ladder. Default
                False preserves headers verbatim — meaning a previously
                max-retried message is terminal after ONE failed attempt and
                returns to the DLQ.
            limit: Maximum number of messages to fetch this call (None =
                drain until empty). Set this when a LIVE consumer on the
                target can fail a replayed message back into this same DLQ
                faster than the drain completes — the held-until-drained
                termination guarantee below covers self-refetch, but not a
                message that genuinely re-arrives via dead-lettering
                mid-drain, which an unbounded loop would replay again.
            allow_self_replay: Allow republishing into *queue* itself. Off by
                default: a message whose origin can't be determined would
                otherwise go straight back onto the queue being drained, and
                with ``limit=None`` the drain never ends. Such messages are
                left on the DLQ and counted in ``ReplayResult.skipped``.

        Returns:
            :class:`ReplayResult` — int-compatible replayed count, with
            ``.failed`` (left on the DLQ), ``.requeued`` (non-matching) and
            ``.skipped`` (no safe destination, left on the DLQ).

        Raises:
            UnsafeToBrowseError: For a quorum queue with a delivery limit, or
                one that can't be verified, since every requeue counts as a
                delivery. With a ``predicate`` the queue is checked up front
                (non-matching messages are requeued by design). Without
                one, replay acks what it republishes and checks only at the
                first message it has to requeue (a failed or skipped one);
                what was already replayed stays replayed.

        Loop Engineering Review, Reliability: a non-matching or
        failed-publish message is **not** nacked (requeued) until after this
        method's fetch loop has fully exhausted the queue. ``basic_get`` has
        no natural "already seen this delivery" tracking of its own -- if
        such a message were requeued immediately, and nothing else is
        consuming from this queue, the very next ``basic_get`` call in this
        same loop could immediately re-fetch that exact message, forever.
        Held-but-unsettled messages are invisible to further ``basic_get``
        calls (the broker still considers them delivered-but-unacked), so
        deferring the nack until after the loop truly exits guarantees
        termination regardless of how many messages the predicate rejects or
        the publisher fails. The requeue runs in a ``finally``, so a raising
        predicate or a failed ``basic_get`` can't strand held messages.
        """
        # A filtered replay requeues by design: vet up front. An unfiltered
        # one acks what it republishes and vets lazily, at its first requeue.
        vetted: bool | None = self._ensure_browsable(queue) if predicate is not None else None
        self._check_partial_replay(queue, predicate, limit)
        replayed = 0
        held_for_requeue: list[RabbitMessage] = []
        failed = 0
        requeued = 0
        skipped = 0
        fetched = 0

        with self._session() as session:
            try:
                while limit is None or fetched < limit:
                    msg = session.basic_get(queue)
                    if msg is None:
                        break
                    fetched += 1
                    held_for_requeue.append(msg)  # until it is acked below

                    if vetted is not None and self._tripwire(queue, msg, vetted):
                        self._raise_tripwire(queue)

                    # Apply predicate filter -- hold, don't nack yet (see docstring).
                    if predicate is not None and not predicate(msg):
                        requeued += 1
                        continue

                    envelope = self._build_replay_envelope(msg, target_queue, target_exchange, reset_retry_count)
                    if self._is_self_replay(queue, envelope, allow_self_replay):
                        skipped += 1
                        vetted = self._vet_requeue(queue, msg, vetted)
                        continue
                    outcome = self._transport.publish(envelope)
                    # A None outcome (duck-typed transport returning nothing) is an
                    # UNVERIFIED publish — treat as failure, never ack against it.
                    if outcome is None or not outcome.ok:
                        # Republish failed — DO NOT ack, or the message is lost
                        # forever. Hold for nack-requeue so it stays on the DLQ.
                        logger.error(
                            "DLQ replay publish failed (status=%s); message stays on %r: routing_key=%s message_id=%s",
                            getattr(outcome, "status", "unknown"),
                            queue,
                            envelope.routing_key,
                            envelope.message_id,
                        )
                        failed += 1
                        vetted = self._vet_requeue(queue, msg, vetted)
                        continue

                    # Ack the original — it is safely republished now
                    if not msg.is_settled:
                        msg.ack()
                    held_for_requeue.pop()
                    replayed += 1
            finally:
                # The fetch loop is done (or failed) -- now it's safe to
                # requeue held messages; this loop can no longer re-fetch them.
                _release_messages(held_for_requeue)

        return ReplayResult(replayed, failed=failed, requeued=requeued, skipped=skipped)

    def purge(self, queue: str) -> int:
        """Purge all messages from the queue.

        Returns the number of messages purged.
        """
        return int(self._transport.purge_queue(queue))

    # ── Async methods ────────────────────────────────────────────────────

    async def peek_async(self, queue: str, limit: int = 10) -> list[RabbitMessage]:
        """Async variant of ``peek``. Cancellation also requeues what was held."""
        vetted = await self._ensure_browsable_async(queue)
        messages: list[RabbitMessage] = []
        async with self._session_async() as session:
            try:
                for _ in range(self._fetch_limit(queue, limit)):
                    msg = await session.basic_get(queue)
                    if msg is None:
                        break
                    messages.append(msg)
                    if self._tripwire(queue, msg, vetted):
                        self._raise_tripwire(queue)
                self._check_whole(queue, len(messages))
            finally:
                await _release_messages_async(messages)
        return messages[:limit]

    async def replay_async(
        self,
        queue: str,
        predicate: Callable[[RabbitMessage], bool] | None = None,
        target_queue: str | None = None,
        target_exchange: str | None = None,
        *,
        reset_retry_count: bool = False,
        limit: int | None = None,
        allow_self_replay: bool = False,
    ) -> ReplayResult:
        """Async variant of ``replay`` -- see its docstring for the
        outcome-checked ack, ``reset_retry_count``, ``allow_self_replay``,
        the delivery-limit check, and why non-matching / failed messages are
        held, not nacked, until the fetch loop has fully exhausted the queue
        (termination guarantee)."""
        vetted: bool | None = await self._ensure_browsable_async(queue) if predicate is not None else None
        self._check_partial_replay(queue, predicate, limit)
        replayed = 0
        held_for_requeue: list[RabbitMessage] = []
        failed = 0
        requeued = 0
        skipped = 0
        fetched = 0

        async with self._session_async() as session:
            try:
                while limit is None or fetched < limit:
                    msg = await session.basic_get(queue)
                    if msg is None:
                        break
                    fetched += 1
                    held_for_requeue.append(msg)

                    if vetted is not None and self._tripwire(queue, msg, vetted):
                        self._raise_tripwire(queue)

                    if predicate is not None and not predicate(msg):
                        requeued += 1
                        continue

                    envelope = self._build_replay_envelope(msg, target_queue, target_exchange, reset_retry_count)
                    if self._is_self_replay(queue, envelope, allow_self_replay):
                        skipped += 1
                        vetted = await self._vet_requeue_async(queue, msg, vetted)
                        continue
                    outcome = await self._transport.publish(envelope)
                    # None outcome = unverified publish = failure (see sync variant).
                    if outcome is None or not outcome.ok:
                        logger.error(
                            "DLQ replay publish failed (status=%s); message stays on %r: routing_key=%s message_id=%s",
                            getattr(outcome, "status", "unknown"),
                            queue,
                            envelope.routing_key,
                            envelope.message_id,
                        )
                        failed += 1
                        vetted = await self._vet_requeue_async(queue, msg, vetted)
                        continue

                    if not msg.is_settled:
                        await msg.ack_async()
                    held_for_requeue.pop()
                    replayed += 1
            finally:
                await _release_messages_async(held_for_requeue)

        return ReplayResult(replayed, failed=failed, requeued=requeued, skipped=skipped)

    async def purge_async(self, queue: str) -> int:
        """Async variant of ``purge``."""
        return int(await self._transport.purge_queue(queue))
