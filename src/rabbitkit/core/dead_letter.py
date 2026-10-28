"""Dead-letter target verification -- never reject into a missing DLQ.

Transport-free (hard invariant 1): the probe is an injected awaitable.

Why this exists: a classic queue dead-letters at most once. When a consumer
rejects a message (``requeue=False``) and the dead-letter target does not
exist -- the DLQ was deleted, or never created under a passive topology --
RabbitMQ drops the message without telling anyone. The broker confirms
nothing on the consumer side, so the client cannot detect it afterwards.

:class:`DeadLetterGuard` closes that hole for the dead-letter topology
rabbitkit itself sets up (default exchange ``""`` plus a routing key naming
the DLQ, which is what the retry and ``auto_provision`` safety paths
declare): before the pipeline dead-letters a message it asks the guard
whether the DLQ queue exists. If it does not, the pipeline requeues the
message instead (with a backoff) and logs an error, so the message survives
until an operator restores the DLQ.

Limits, stated plainly:

* Only a target reachable through the default exchange can be verified (the
  routing key IS the queue name). A dead-letter exchange with its own
  bindings is not checked -- :meth:`DeadLetterGuard.check` returns True for
  it -- because verifying the binding table needs the management API.
* A positive answer is cached for ``ttl`` seconds, so a DLQ deleted inside
  that window can still drop messages. Negative answers are never cached:
  every dead-letter attempt re-probes until the DLQ is back.
* A probe that fails (connection down) counts as "missing" (fail closed):
  requeueing is always safe, dead-lettering into the unknown is not.

Before any of that, the async pipeline tries a :class:`DeadLetterCopier`
copy: a confirmed copy with rabbitkit's failure metadata, after
which the delivery is acked. The guard decides only when that copy could
not be confirmed.

Exception: a quorum source declared with ``dead_letter_strategy=
AT_LEAST_ONCE`` is not probed. The broker itself keeps a dead-lettered
message on the source until the target confirms it, so rejecting is already
loss-free, and requeueing would only run the handler again for nothing.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable, Callable

from rabbitkit.core.errors import classify_error
from rabbitkit.core.failure_copy import (
    DEFAULT_RETRY_HEADER,
    ERROR_MESSAGE_MAX_LEN,
    H_ORIGINAL_QUEUE,
    dead_letter_envelope,
)
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.sanitizer import ErrorSanitizer
from rabbitkit.core.types import (
    DeadLetterStrategy,
    FailureCategory,
    MessageEnvelope,
    PublishOutcome,
    PublishStatus,
    QueueType,
)

logger = logging.getLogger(__name__)

QueueProbe = Callable[[str], Awaitable[bool]]


class DeadLetterGuard:
    """Verify, with a short cache, that a route's dead-letter queue exists.

    Args:
        probe: ``async (queue_name) -> bool`` -- True when the queue exists.
            Must not raise for a missing queue; any exception is treated as
            "cannot verify" and therefore as missing.
        ttl: seconds a positive probe result is trusted.
        clock: injectable monotonic clock (tests).
    """

    def __init__(
        self,
        probe: QueueProbe,
        *,
        ttl: float = 5.0,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._probe = probe
        self._ttl = ttl
        self._clock = clock
        self._targets: dict[str, str | None] = {}
        self._verified_until: dict[str, float] = {}
        self.probes = 0
        self.missing = 0

    def set_target(self, queue: str, exchange: str | None, routing_key: str | None) -> None:
        """Record the dead-letter arguments *queue* was declared with.

        Only ``exchange == ""`` with a routing key is verifiable (the key is
        the DLQ's name). Anything else records "unverifiable".
        """
        self._targets[queue] = routing_key if exchange == "" and routing_key else None
        self._verified_until.pop(queue, None)

    def target_for(self, route: RouteDefinition) -> str | None:
        """The DLQ name a rejected delivery from *route* is routed to, if
        rabbitkit can verify it; None otherwise."""
        name = route.queue.name
        if name in self._targets:
            return self._targets[name]
        q = route.queue
        if q.dead_letter_exchange == "" and q.dead_letter_routing_key:
            return q.dead_letter_routing_key
        return None

    async def check(self, route: RouteDefinition) -> bool:
        """True when dead-lettering from *route* will reach a queue (or the
        target cannot be verified at all); False when the DLQ is missing or
        the probe failed."""
        target = self.target_for(route)
        if target is None or _broker_retains_dead_letters(route):
            return True
        now = self._clock()
        if self._verified_until.get(target, 0.0) > now:
            return True
        self.probes += 1
        try:
            exists = bool(await self._probe(target))
        except Exception as exc:  # fail closed: requeueing is always safe
            logger.warning("Could not verify dead-letter queue %r: %s", target, exc)
            exists = False
        if exists:
            self._verified_until[target] = now + self._ttl
            return True
        self._verified_until.pop(target, None)
        self.missing += 1
        return False


def _broker_retains_dead_letters(route: RouteDefinition) -> bool:
    """True when the broker guarantees the dead-letter hop itself: a quorum
    queue with ``x-dead-letter-strategy=at-least-once`` (which
    ``RabbitQueue`` only accepts together with ``overflow=reject-publish``)."""
    q = route.queue
    return q.queue_type is QueueType.QUORUM and q.dead_letter_strategy is DeadLetterStrategy.AT_LEAST_ONCE


class DeadLetterCopier:
    """Confirmed, metadata-stamped DLQ copies for failures that bypass
    ``RetryMiddleware``.

    An ``on_receive`` failure (bad gzip, bad signature), a body over
    ``ConsumerConfig.max_body_bytes``, a failure on a route without retry, or
    a transient failure on redelivery used to reach the DLQ only through the
    broker's dead-lettering of a reject, carrying ``x-death`` and nothing
    else. :meth:`copy` publishes the same copy ``RetryMiddleware``'s terminal
    path does (wire body and encoding, every property and header, a stable
    ``message_id``, ``x-rabbitkit-error-category`` / ``-error-class`` /
    ``-error-message`` (sanitized) / ``-failure-category`` / ``-attempts`` /
    ``-first-failed-at`` / ``-last-failed-at`` / ``-original-queue`` /
    ``-original-exchange`` / ``-original-routing-key`` /
    ``-original-message-id`` / ``-dead-lettered-at`` and the retry count),
    ``mandatory=True``, to the DLQ the route dead-letters into.

    Returns True only for a CONFIRMED copy; the caller then acks the
    delivery. Anything else (no addressable DLQ, a RETURNED / NACKED /
    unknown outcome, a raising publish) returns False and the caller falls
    back to rejecting into the DLX (or requeueing when the DLQ is missing).
    A copy that landed without its confirm can leave a duplicate in the DLQ
    after that fallback; the delivery itself is never lost.

    Only a DLQ reached through the default exchange (``""`` + routing key =
    queue name, what the retry and ``auto_provision`` paths declare) can be
    addressed, as for :class:`DeadLetterGuard`.

    Args:
        publish: confirmed publish, e.g. the broker's internal publish.
        target_for: route -> DLQ queue name or None (``DeadLetterGuard.target_for``).
        sanitizer_for: route -> the ``ErrorSanitizer`` for its error headers.
        retry_header_for: route -> the retry-count header name.
    """

    def __init__(
        self,
        publish: Callable[[MessageEnvelope], Awaitable[PublishOutcome]],
        target_for: Callable[[RouteDefinition], str | None],
        *,
        sanitizer_for: Callable[[RouteDefinition], ErrorSanitizer] | None = None,
        retry_header_for: Callable[[RouteDefinition], str] | None = None,
    ) -> None:
        self._publish = publish
        self._target_for = target_for
        default_sanitizer = ErrorSanitizer(max_summary_len=ERROR_MESSAGE_MAX_LEN)
        self._sanitizer_for = sanitizer_for or (lambda _route: default_sanitizer)
        self._retry_header_for = retry_header_for or (lambda _route: DEFAULT_RETRY_HEADER)
        self.copied = 0
        self.failed = 0

    async def copy(self, route: RouteDefinition, message: RabbitMessage, exc: BaseException, reason: str) -> bool:
        """Publish the DLQ copy of *message*; True when it is CONFIRMED."""
        dlq = self._target_for(route)
        if dlq is None:
            return False
        envelope = dead_letter_envelope(
            message,
            dlq=dlq,
            exc=exc,
            classified=classify_error(exc),
            # A transient failure is dead-lettered here only when it failed
            # again on redelivery: its two-strike budget is spent.
            category=FailureCategory.EXHAUSTED if reason == "transient_on_redelivery" else FailureCategory.PERMANENT,
            attempts=2 if message.redelivered else 1,
            sanitizer=self._sanitizer_for(route),
            header_defaults={H_ORIGINAL_QUEUE: route.queue.name, self._retry_header_for(route): 0},
        )
        try:
            outcome = await self._publish(envelope)
        except asyncio.CancelledError:
            raise
        except Exception as publish_exc:  # the fallback (reject / requeue) still settles it
            self.failed += 1
            logger.warning(
                "DLQ copy to %r raised %s; falling back to the broker dead-letter path",
                dlq,
                type(publish_exc).__name__,
            )
            return False
        if outcome.status is not PublishStatus.CONFIRMED:
            self.failed += 1
            logger.warning(
                "DLQ copy to %r not confirmed (%s, reason=%s); falling back to the broker dead-letter path, "
                "which carries only x-death",
                dlq,
                outcome.status.value,
                outcome.reason,
            )
            return False
        self.copied += 1
        return True
