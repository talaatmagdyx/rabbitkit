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
"""

from __future__ import annotations

import logging
import time
from collections.abc import Awaitable, Callable

from rabbitkit.core.route import RouteDefinition

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
        if target is None:
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
