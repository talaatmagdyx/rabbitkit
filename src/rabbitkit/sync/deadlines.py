"""``ConsumerConfig.processing_timeout`` for the sync broker.

Python cannot interrupt a thread, so the sync broker cannot cancel a handler
the way the async broker cancels its task. What it can do, and does:

* Handlers run off the I/O thread (the worker pool; with ``worker_count=1``
  one dedicated handler thread), so the I/O thread stays free.
* When a handler's deadline passes, the I/O (owner) thread settles its
  delivery through the pipeline's normal failure path with
  :class:`~rabbitkit.core.errors.HandlerTimeoutError` (``UNKNOWN_OUTCOME``,
  transient): requeued once, dead-lettered on the redelivery, exactly as
  the async broker settles a cancelled handler.
* The handler thread is NOT stopped. It keeps its worker slot until it
  returns, so no more than ``worker_count`` handlers ever run at once.
* Settlement is claimed atomically per delivery (:class:`DeliveryClaim`):
  whichever of the handler and the deadline settles first owns the delivery,
  and the other one's settlement is dropped. When the deadline won, the
  handler's late ack/nack/reject never reaches the channel, its internal
  publishes (handler result, retry or DLQ copy) are refused
  (``NOT_SENT``, reason ``cancelled``), and its late return is logged and
  discarded.

:meth:`SyncDeadlines.begin` / :meth:`~SyncDeadlines.finish` run on the
handler thread; :meth:`~SyncDeadlines.check` runs on the connection's owner
thread (the consume loop's per-tick hook).
"""

from __future__ import annotations

import logging
import threading
import time
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

from rabbitkit.core.errors import HandlerTimeoutError

if TYPE_CHECKING:
    from rabbitkit.core.message import RabbitMessage
    from rabbitkit.core.route import RouteDefinition

logger = logging.getLogger(__name__)

_PENDING = 0
_HANDLER = 1  # the handler's own settlement claimed the delivery first
_DEADLINE = 2  # the deadline claimed it; the handler's settlement is dropped


class LateSettlementError(BaseException):
    """Raised on a handler thread that tries to settle a delivery its
    processing deadline already settled.

    A ``BaseException`` (like ``CancelledError``), so the pipeline's and the
    user's ``except Exception`` blocks do not treat it as a handler failure
    and settle again. The broker catches it around the handler run.
    """


class DeliveryClaim:
    """Who settles one delivery: its handler or its processing deadline."""

    __slots__ = ("_lock", "_settler", "_state", "deadline", "message", "route", "started")

    def __init__(self, route: RouteDefinition, message: RabbitMessage, started: float, deadline: float) -> None:
        self.route = route
        self.message = message
        self.started = started
        self.deadline = deadline
        self._lock = threading.Lock()
        self._state = _PENDING
        self._settler: int | None = None

    @property
    def expired(self) -> bool:
        """True once the deadline claimed (and settled) the delivery."""
        return self._state == _DEADLINE

    def claim_for_deadline(self) -> bool:
        """Claim the delivery for the deadline (calling thread settles it).

        False when the handler already claimed it: its settlement is under way.
        """
        with self._lock:
            if self._state != _PENDING:
                return False
            self._state = _DEADLINE
            self._settler = threading.get_ident()
            return True

    def hand_back(self) -> None:
        """The deadline did not settle after all: the handler owns the delivery again."""
        with self._lock:
            self._state = _HANDLER
            self._settler = None

    def guard(self, real: Callable[..., None]) -> Callable[..., None]:
        """Wrap a transport settlement function with the claim check."""

        def guarded(*args: Any) -> None:
            with self._lock:
                if self._state == _PENDING:
                    self._state = _HANDLER
                elif self._state == _DEADLINE and threading.get_ident() != self._settler:
                    raise LateSettlementError
            real(*args)

        return guarded


class SyncDeadlines:
    """Per-delivery processing deadlines of one sync broker (see module doc).

    Args:
        seconds: ``ConsumerConfig.processing_timeout``.
        on_expired: Settles an expired delivery on the owner thread
            (``HandlerPipeline.settle_expired_sync``); False when the
            policy leaves it to the handler.
        clock: Monotonic clock (injectable for tests).
    """

    def __init__(
        self,
        seconds: float,
        on_expired: Callable[[RouteDefinition, RabbitMessage, HandlerTimeoutError], bool],
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.seconds = seconds
        self._on_expired = on_expired
        self._clock = clock
        self._lock = threading.Lock()
        self._claims: dict[int, DeliveryClaim] = {}
        self._local = threading.local()
        #: Deliveries settled by their deadline (an observable counter).
        self.expired_total = 0

    @property
    def poll_interval(self) -> float:
        """How often the owner thread should check (bounds the lateness)."""
        return max(0.01, min(1.0, self.seconds / 4))

    @property
    def running(self) -> int:
        """Handlers currently under a deadline (late ones included)."""
        with self._lock:
            return len(self._claims)

    # ── handler thread ───────────────────────────────────────────────────

    def begin(self, route: RouteDefinition, message: RabbitMessage) -> DeliveryClaim:
        """Start *message*'s deadline and guard its settlement (handler thread)."""
        now = self._clock()
        claim = DeliveryClaim(route, message, now, now + self.seconds)
        if message._ack_fn is not None:
            message._ack_fn = claim.guard(message._ack_fn)
        if message._nack_fn is not None:
            message._nack_fn = claim.guard(message._nack_fn)
        if message._reject_fn is not None:
            message._reject_fn = claim.guard(message._reject_fn)
        # The deadline path and a late handler both report the settlement
        # metric; only the first one counts it.
        message._settlement_metric_once = [None]
        with self._lock:
            self._claims[id(claim)] = claim
        self._local.claim = claim
        return claim

    def finish(self, claim: DeliveryClaim) -> None:
        """The handler run returned (handler thread). Logs a late return."""
        self._local.claim = None
        with self._lock:
            self._claims.pop(id(claim), None)
        if claim.expired:
            logger.warning(
                "Handler for queue=%s message_id=%s delivery_tag=%s returned after %.1fs, past its "
                "processing_timeout of %.1fs; the delivery was already settled at the deadline, so its "
                "result is discarded",
                claim.route.queue.name,
                claim.message.message_id,
                claim.message.delivery_tag,
                self._clock() - claim.started,
                self.seconds,
            )

    def current_expired(self) -> bool:
        """True on a handler thread whose delivery its deadline already settled."""
        claim: DeliveryClaim | None = getattr(self._local, "claim", None)
        return claim is not None and claim.expired

    # ── owner thread ─────────────────────────────────────────────────────

    def check(self) -> None:
        """Settle every delivery whose deadline passed (owner thread only)."""
        now = self._clock()
        with self._lock:
            due = [c for c in self._claims.values() if c.deadline <= now and not c.expired]
        for claim in due:
            if not claim.claim_for_deadline():
                continue  # the handler is settling it right now
            message = claim.message
            if message.channel_alive is False:
                # The channel is gone: the broker already requeued the delivery.
                logger.debug("processing_timeout passed for a delivery on a closed channel; nothing to settle")
                continue
            self.expired_total += 1
            logger.warning(
                "Handler for queue=%s message_id=%s delivery_tag=%s exceeded processing_timeout=%.1fs; "
                "settling the delivery now (outcome unknown). The handler thread cannot be stopped: it "
                "keeps its worker until it returns, and its late result is discarded",
                claim.route.queue.name,
                message.message_id,
                message.delivery_tag,
                self.seconds,
            )
            try:
                if not self._on_expired(claim.route, message, HandlerTimeoutError(self.seconds)):
                    claim.hand_back()
            except Exception:
                # Per-tick hook on the I/O thread: one delivery's settlement
                # failure must not stop the consume loop or the other checks.
                logger.exception("Settling an expired delivery failed (queue=%s)", claim.route.queue.name)


__all__ = ["DeliveryClaim", "LateSettlementError", "SyncDeadlines"]
