"""Handler invocation pipeline — orchestrates message processing.

Executes the full message processing pipeline:
- See Contract 3 (Middleware Ordering) for exact chain.
- See Contract 4 (Parameter Resolution) for DI rules.
- See Contract 5 (Result Publishing) for publish precedence.
- See Contract 1 (AckPolicy) for ack behavior.

Pipeline calls msg.ack() or await msg.ack_async() depending on transport type.
Decompression operates on message.body before deserialize.
"""

from __future__ import annotations

import dataclasses
import logging
import threading
import time
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

import structlog

from rabbitkit.core.errors import classify_error
from rabbitkit.core.message import AckMessage, NackMessage, RabbitMessage, RejectMessage, is_rabbit_message_annotation
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.types import (
    REQUEUED_FOR_RETRY,
    AckPolicy,
    AckStrategy,
    ErrorSeverity,
    MessageEnvelope,
    PublishOutcome,
)
from rabbitkit.di.context import ContextRepo
from rabbitkit.di.resolver import DependencyScope, DIResolver
from rabbitkit.serialization.base import Serializer

logger = structlog.stdlib.get_logger(__name__)
_stdlib_logger = logging.getLogger(__name__)

# Transport "channel/connection died" exception class names. Matched by NAME
# (not isinstance) because core/ never imports pika or aio-pika. Covers pika
# (ChannelWrongStateError, ChannelClosed*, ConnectionClosed*, StreamLostError)
# and aio-pika/aiormq (ChannelInvalidStateError, AMQPConnectionError).
_CHANNEL_GONE_NAMES = frozenset(
    {
        "ChannelWrongStateError",
        "ChannelClosed",
        "ChannelClosedByBroker",
        "ChannelClosedByClient",
        "ChannelInvalidStateError",
        "ConnectionClosed",
        "ConnectionClosedByBroker",
        "ConnectionWrongStateError",
        "AMQPConnectionError",
        "StreamLostError",
    }
)


def _is_channel_gone(exc: BaseException) -> bool:
    """True if *exc* (or its cause chain) is a transport channel/connection-death error."""
    seen: set[int] = set()
    current: BaseException | None = exc
    while current is not None and id(current) not in seen:
        if type(current).__name__ in _CHANNEL_GONE_NAMES:
            return True
        seen.add(id(current))
        current = current.__cause__ or current.__context__
    return False

class _SettlementLossWarner:
    """Aggregate "channel closed before settlement" warnings.

    A channel loss strands the entire prefetch window at once — warning per
    stranded message turns one broker bounce into `prefetch` identical log
    lines (observed: 20 lines for a prefetch-20 chaos bounce; a production
    prefetch-200 consumer would emit 200). Instead: the FIRST occurrence in
    a window logs the full warning immediately (a single event is never
    hidden or delayed), repeats within the window are counted (and logged
    at DEBUG for forensics), and the count is flushed as one summary line
    when the next occurrence arrives after the window closes.

    Thread-safe: the sync pipeline settles from worker-pool threads.
    """

    WINDOW_SECONDS = 5.0

    def __init__(self, clock: Callable[[], float] = time.monotonic, log: Any = None) -> None:
        self._clock = clock
        # Injectable for deterministic tests (module logger has global
        # structlog config/caching state that makes capture-based
        # assertions order-dependent across a full suite run).
        self._log = log if log is not None else logger
        self._lock = threading.Lock()
        self._window_start = -self.WINDOW_SECONDS  # first call always opens a window
        self._suppressed = 0

    def warn(self, exc_name: str) -> None:
        now = self._clock()
        with self._lock:
            if now - self._window_start > self.WINDOW_SECONDS:
                if self._suppressed:
                    self._log.warning(
                        "... and %d more messages were left unsettled for broker "
                        "redelivery in the previous %.0fs window (same channel-loss "
                        "event; per-message details at DEBUG)",
                        self._suppressed,
                        self.WINDOW_SECONDS,
                    )
                self._window_start = now
                self._suppressed = 0
                self._log.warning(
                    "Channel closed before settlement (%s); leaving message "
                    "unsettled — the broker will redeliver it (at-least-once). "
                    "Further occurrences in the next %.0fs are aggregated.",
                    exc_name,
                    self.WINDOW_SECONDS,
                )
            else:
                self._suppressed += 1
                self._log.debug(
                    "Channel closed before settlement (%s); message left unsettled "
                    "for redelivery (occurrence %d in the current window)",
                    exc_name,
                    self._suppressed + 1,
                )


# M10: on the async path, bodies at/above this size are decoded in a worker
# thread (asyncio.to_thread) so a large JSON/msgspec/pydantic parse doesn't
# block the event loop. Below it, inline decode is faster than the thread hop.
# ponytail: fixed 256 KiB threshold — make it configurable only if a workload
# proves the cutoff wrong.
_DECODE_OFFLOAD_THRESHOLD_BYTES = 256 * 1024


def _emit_settlement_metric(route: RouteDefinition, message: RabbitMessage) -> None:
    """Emit the ack/nack/reject counter for *message*, if a
    ``MetricsMiddleware`` is present on *route* (M2).

    ``MetricsMiddleware.consume_scope``/``consume_scope_async`` only wrap
    handler execution; final settlement is decided by this pipeline's own
    ack-orchestration code, which runs AFTER that wrapped call returns —
    so the middleware itself can never observe the final disposition, and
    ``messages_acked_total``/``nacked_total``/``rejected_total`` would
    otherwise be defined but never emitted. A local, lazy import is used
    (not a module-level one) so ``core/`` does not gain a hard dependency
    on ``middleware/`` — this is the one place core reaches up to an
    optional, purely-observational integration, and only when a
    ``MetricsMiddleware`` is actually configured on the route.

    A "pending" disposition (e.g. a MANUAL-policy handler that hasn't
    settled its message yet by the time this runs) emits nothing — there is
    nothing final to report yet.
    """
    if message.disposition == "pending":
        return
    from rabbitkit.middleware.metrics import MetricsMiddleware

    for mw in route.route_middlewares:
        if isinstance(mw, MetricsMiddleware):
            mw.record_settlement(message, message.disposition)
            return


def _log_result_publish_failure(message: RabbitMessage, outcome: PublishOutcome) -> None:
    """Log a failed result publish (L1).

    The caller (``_publish_result_sync``/``_publish_result_async``) nacks
    the source message with ``requeue=True`` on failure, re-running the
    handler (including any side effects) on redelivery. ``message.redelivered``
    is ``True`` when THIS delivery is itself already a redelivery — logging
    at ERROR (vs. WARNING on a first attempt) makes a sustained publish
    outage, which would otherwise hot-loop this nack+requeue silently,
    loud and alertable via log-based monitoring instead of only a stream
    of routine-looking WARNINGs.
    """
    log = logger.error if message.redelivered else logger.warning
    log(
        "Result publish failed%s: status=%s, exchange=%s, routing_key=%s",
        " (message already redelivered once -- this nack+requeue is repeating; "
        "verify broker health and that the handler is idempotent under repeated "
        "execution)"
        if message.redelivered
        else "",
        outcome.status,
        outcome.exchange,
        outcome.routing_key,
    )


# ── AckPolicy strategy dispatch ──────────────────────────────────────────
# Replaces the per-call if/elif chains over AckPolicy with a single dict
# lookup. Each strategy owns the success-path ack and the error-path
# settlement; handler-raised AckMessage/NackMessage/RejectMessage stay in
# the pipeline (they are handler-driven, not policy-driven).


class _AutoStrategy:
    """AUTO: success→ack, exception→classify→nack(requeue)/reject."""

    acks_first: bool = False

    def on_success(self, msg: RabbitMessage) -> None:
        if not msg.is_settled:
            msg.ack()

    def on_error(self, msg: RabbitMessage, exc: Exception) -> None:
        classified = classify_error(exc)
        if classified.severity == ErrorSeverity.TRANSIENT:
            msg.nack(requeue=True)
        else:
            msg.reject(requeue=False)


class _ManualStrategy:
    """MANUAL: handler owns settlement ENTIRELY; pipeline never auto-settles on success.

    M11: ``on_success`` previously did ``if not msg.is_settled: msg.ack()``
    — contradicting this class's own documented "handler owns settlement"
    contract. A MANUAL handler that intentionally defers settlement (e.g.
    hands the message to another task/thread to ack later) got an
    unexpected ack right here — a real loss risk: if the process crashes
    before that deferred settlement actually runs, the message is gone
    (already acked) instead of being redelivered.
    """

    acks_first: bool = False

    def on_success(self, msg: RabbitMessage) -> None:
        if not msg.is_settled:
            logger.warning(
                "MANUAL ack policy: handler returned without settling the message "
                "(no ack()/nack()/reject() called) — left unsettled, not auto-acked. "
                "If settlement is deferred intentionally, ignore this; otherwise the "
                "handler is missing a settlement call."
            )

    def on_error(self, msg: RabbitMessage, exc: Exception) -> None:
        logger.error("Unhandled exception in MANUAL mode handler: %s", exc)
        raise


class _NackOnErrorStrategy:
    """NACK_ON_ERROR: success→ack, exception→nack(requeue=False)."""

    acks_first: bool = False

    def on_success(self, msg: RabbitMessage) -> None:
        if not msg.is_settled:
            msg.ack()

    def on_error(self, msg: RabbitMessage, exc: Exception) -> None:
        msg.nack(requeue=False)


class _AckFirstStrategy:
    """ACK_FIRST: ack BEFORE the handler runs (at-most-once)."""

    acks_first: bool = True

    def on_success(self, msg: RabbitMessage) -> None:
        if not msg.is_settled:
            msg.ack()

    def on_error(self, msg: RabbitMessage, exc: Exception) -> None:
        # Unreachable in practice — the message is pre-acked before the handler
        # runs, so _handle_*_exception returns early on is_settled. Classify
        # like AUTO for defensiveness if ever called on an unsettled message.
        classified = classify_error(exc)
        if classified.severity == ErrorSeverity.TRANSIENT:
            msg.nack(requeue=True)
        else:
            msg.reject(requeue=False)


_ACK_STRATEGIES: dict[AckPolicy, AckStrategy] = {
    AckPolicy.AUTO: _AutoStrategy(),
    AckPolicy.MANUAL: _ManualStrategy(),
    AckPolicy.NACK_ON_ERROR: _NackOnErrorStrategy(),
    AckPolicy.ACK_FIRST: _AckFirstStrategy(),
}


class _AsyncAckStrategy(Protocol):
    """Async counterpart of ``AckStrategy`` for the async pipeline."""

    @property
    def acks_first(self) -> bool: ...

    async def on_success(self, msg: RabbitMessage) -> None: ...

    async def on_error(self, msg: RabbitMessage, exc: Exception) -> None: ...


class _AutoStrategyAsync:
    acks_first: bool = False

    async def on_success(self, msg: RabbitMessage) -> None:
        if not msg.is_settled:
            await msg.ack_async()

    async def on_error(self, msg: RabbitMessage, exc: Exception) -> None:
        classified = classify_error(exc)
        if classified.severity == ErrorSeverity.TRANSIENT:
            await msg.nack_async(requeue=True)
        else:
            await msg.reject_async(requeue=False)


class _ManualStrategyAsync:
    """MANUAL: handler owns settlement ENTIRELY (M11 — see ``_ManualStrategy``'s
    docstring for why ``on_success`` must not auto-ack)."""

    acks_first: bool = False

    async def on_success(self, msg: RabbitMessage) -> None:
        if not msg.is_settled:
            logger.warning(
                "MANUAL ack policy: handler returned without settling the message "
                "(no ack()/nack()/reject() called) — left unsettled, not auto-acked. "
                "If settlement is deferred intentionally, ignore this; otherwise the "
                "handler is missing a settlement call."
            )

    async def on_error(self, msg: RabbitMessage, exc: Exception) -> None:
        logger.error("Unhandled exception in MANUAL mode handler: %s", exc)
        raise


class _NackOnErrorStrategyAsync:
    acks_first: bool = False

    async def on_success(self, msg: RabbitMessage) -> None:
        if not msg.is_settled:
            await msg.ack_async()

    async def on_error(self, msg: RabbitMessage, exc: Exception) -> None:
        await msg.nack_async(requeue=False)


class _AckFirstStrategyAsync:
    acks_first: bool = True

    async def on_success(self, msg: RabbitMessage) -> None:
        if not msg.is_settled:
            await msg.ack_async()

    async def on_error(self, msg: RabbitMessage, exc: Exception) -> None:
        classified = classify_error(exc)
        if classified.severity == ErrorSeverity.TRANSIENT:
            await msg.nack_async(requeue=True)
        else:
            await msg.reject_async(requeue=False)


_ACK_STRATEGIES_ASYNC: dict[AckPolicy, _AsyncAckStrategy] = {
    AckPolicy.AUTO: _AutoStrategyAsync(),
    AckPolicy.MANUAL: _ManualStrategyAsync(),
    AckPolicy.NACK_ON_ERROR: _NackOnErrorStrategyAsync(),
    AckPolicy.ACK_FIRST: _AckFirstStrategyAsync(),
}


class HandlerPipeline:
    """Executes the full message processing pipeline.

    on_receive() receives a RabbitMessage (transport builds it first).

    The pipeline is responsible for:
    1. Ack timing (per AckPolicy)
    2. Deserialization (via serializer)
    3. Parameter resolution (via DI resolver)
    4. Handler invocation
    5. Result serialization and publishing
    6. Settlement (ack/nack/reject)

    Both sync and async variants are provided.
    """

    def __init__(
        self,
        serializer: Serializer[Any] | None = None,
        di_resolver: DIResolver | None = None,
        context_repo: ContextRepo | None = None,
        reject_transient_on_redelivery: bool = False,
    ) -> None:
        self._serializer = serializer
        self._di_resolver = di_resolver
        self._context_repo = context_repo
        # M6: opt-in 2-strike cap on transient hot-loops (see ConsumerConfig).
        self._reject_transient_on_redelivery = reject_transient_on_redelivery
        # Per-handler caches so the hot path avoids inspect.signature() per message.
        self._body_type_cache: dict[Any, type | None] = {}
        self._sig_cache: dict[Any, Any] = {}  # handler -> inspect.Signature (fallback resolver)
        # Auto-DI: when no resolver is passed, handlers that use Depends/Header/Path/
        # Context markers get a lazily-created resolver; marker-free handlers keep the
        # fast fallback (so the simple-handler hot path and its behavior are unchanged).
        self._auto_resolver: Any | None = None
        self._needs_di_cache: dict[Any, bool] = {}
        # P3: cache serializer.decode availability per serializer.
        self._has_decode_cache: dict[Any, bool] = {}
        # Aggregate channel-loss settlement warnings (one bounce strands the
        # whole prefetch window; don't emit `prefetch` identical log lines).
        self._settlement_loss_warner = _SettlementLossWarner()
        # P4: precompute parameter binding plan per handler.
        self._binding_plan_cache: dict[Any, list[tuple[str, str]]] = {}
        # P5: cache whether handler is a coroutine function.
        self._is_async_handler_cache: dict[Any, bool] = {}
        # M-P1: cache the composed middleware chain per route — the chain depends
        # only on route.route_middlewares (fixed after registration), so rebuild
        # once per route instead of allocating N closures per message.
        self._consume_chain_cache: dict[int, Callable[[RabbitMessage], Any]] = {}
        self._consume_chain_async_cache: dict[int, Callable[[RabbitMessage], Awaitable[Any]]] = {}
        self._publish_chain_cache: dict[
            int, Callable[[MessageEnvelope, Callable[[MessageEnvelope], PublishOutcome]], Any]
        ] = {}
        self._publish_chain_async_cache: dict[
            int, Callable[[MessageEnvelope, Callable[[MessageEnvelope], Awaitable[PublishOutcome]]], Awaitable[Any]]
        ] = {}
        # C3: broker-level publish middleware chains — keyed by id(middlewares),
        # the SAME list object a broker stores once at construction (see
        # compose_broker_publish_sync/_async). Separate from the route-keyed
        # caches above because broker.publish() is not route-scoped.
        self._broker_publish_chain_cache: dict[
            int, Callable[[MessageEnvelope, Callable[[MessageEnvelope], PublishOutcome]], Any]
        ] = {}
        self._broker_publish_chain_async_cache: dict[
            int, Callable[[MessageEnvelope, Callable[[MessageEnvelope], Awaitable[PublishOutcome]]], Awaitable[Any]]
        ] = {}

    def clear_caches(self) -> None:
        """Drop all per-route caches.

        Clears the four route-keyed middleware-chain caches
        (``_consume_chain_cache``, ``_consume_chain_async_cache``,
        ``_publish_chain_cache``, ``_publish_chain_async_cache``).

        The caches are keyed by ``id(route)`` and are bounded by the number of
        registered routes, which is typically small and stable. They are only
        an eviction concern across reconnect/restart cycles where old
        ``RouteDefinition`` objects are dropped and replaced by new ones; in
        that case the stale entries (keyed by the old ``id``) would otherwise
        linger. Call this on reconnect/restart to reclaim them — the next
        message rebuilds the chain lazily.

        Does NOT clear the broker-level publish chain caches
        (``_broker_publish_chain_cache`` / ``_broker_publish_chain_async_cache``)
        — those are keyed by the broker's ``publish_middlewares`` list, which is
        set once at construction and never mutated in place, so they cannot go
        stale the way route-keyed caches can.
        """
        self._consume_chain_cache.clear()
        self._consume_chain_async_cache.clear()
        self._publish_chain_cache.clear()
        self._publish_chain_async_cache.clear()

    def compose_broker_publish_sync(
        self,
        middlewares: list[Any],
    ) -> Callable[[MessageEnvelope, Callable[[MessageEnvelope], PublishOutcome]], Any]:
        """Compose broker-level ``publish_scope`` middlewares into a reusable chain.

        Unlike :meth:`_compose_publish_sync` (which wraps a route's
        HANDLER-RESULT publishes per Contract 5), this wraps ``broker.publish()``
        itself — the primary producer API — so middleware such as signing or
        tracing actually applies to direct publishes, not just replies/results.

        Cached by ``id(middlewares)``: callers must pass the SAME list object on
        every call (e.g. a broker stores it once at construction) for the cache
        to hit — see :meth:`clear_caches`.
        """
        cached = self._broker_publish_chain_cache.get(id(middlewares))
        if cached is not None:
            return cached

        def leaf(env: MessageEnvelope, fn: Callable[[MessageEnvelope], PublishOutcome]) -> Any:
            return fn(env)

        chain: Callable[[MessageEnvelope, Callable[[MessageEnvelope], PublishOutcome]], Any] = leaf
        for mw in reversed(middlewares):
            nxt = chain

            def wrapped(
                env: MessageEnvelope,
                fn: Callable[[MessageEnvelope], PublishOutcome],
                _mw: Any = mw,
                _nxt: Any = nxt,
            ) -> Any:
                return _mw.publish_scope(lambda e: _nxt(e, fn), env)

            chain = wrapped
        self._broker_publish_chain_cache[id(middlewares)] = chain
        return chain

    def compose_broker_publish_async(
        self,
        middlewares: list[Any],
    ) -> Callable[[MessageEnvelope, Callable[[MessageEnvelope], Awaitable[PublishOutcome]]], Awaitable[Any]]:
        """Async variant of :meth:`compose_broker_publish_sync`."""
        cached = self._broker_publish_chain_async_cache.get(id(middlewares))
        if cached is not None:
            return cached

        async def leaf(
            env: MessageEnvelope,
            fn: Callable[[MessageEnvelope], Awaitable[PublishOutcome]],
        ) -> Any:
            return await fn(env)

        chain: Callable[[MessageEnvelope, Callable[[MessageEnvelope], Awaitable[PublishOutcome]]], Awaitable[Any]] = (
            leaf
        )
        for mw in reversed(middlewares):
            nxt = chain

            async def wrapped(
                env: MessageEnvelope,
                fn: Callable[[MessageEnvelope], Awaitable[PublishOutcome]],
                _mw: Any = mw,
                _nxt: Any = nxt,
            ) -> Any:
                return await _mw.publish_scope_async(lambda e: _nxt(e, fn), env)

            chain = wrapped
        self._broker_publish_chain_async_cache[id(middlewares)] = chain
        return chain

    def process_sync(
        self,
        route: RouteDefinition,
        message: RabbitMessage,
        publish_fn: Callable[[MessageEnvelope], PublishOutcome] | None = None,
    ) -> None:
        """Sync pipeline — calls msg.ack(), handler(), publish().

        Pipeline stages:
        1. ACK_FIRST: ack before handler
        2. Deserialize body → resolve params → call handler
        3. Process result (serialize + publish if applicable)
        4. Settle message (ack/nack/reject per AckPolicy)
        """
        # Filter check — reject before any processing
        if route.filter_fn is not None and not route.filter_fn(message):
            if not message.is_settled:
                message.nack(requeue=False)
            _emit_settlement_metric(route, message)
            return

        # M-P3: only bind contextvars when DEBUG is emitted — avoids per-message
        # dict/token churn on the hot path when structured logging isn't in DEBUG.
        debug = _stdlib_logger.isEnabledFor(logging.DEBUG)
        if debug:
            structlog.contextvars.bind_contextvars(
                message_id=message.message_id,
                routing_key=message.routing_key,
                queue=route.queue.name,
                handler=getattr(route.handler, "__qualname__", repr(route.handler)),
            )

        try:
            strategy = _ACK_STRATEGIES[route.ack_policy]

            # ACK_FIRST: ack before handler runs
            if strategy.acks_first:
                message.ack()

            try:
                # Resolve parameters and call handler (through the middleware chain)
                result = self._run_consume_sync(route, message)

                # Publish result if needed (Contract 5). M7: the
                # REQUEUED_FOR_RETRY sentinel is NOT a handler return value —
                # an inner RetryMiddleware requeued the message and already
                # settled it. Publishing it would serialize the sentinel as a
                # bogus RPC reply/result (once per retry attempt). Skip it.
                if (
                    result is not None
                    and result is not REQUEUED_FOR_RETRY
                    and not self._publish_result_sync(route, message, result, publish_fn)
                ):
                    # Result lost — don't ack. Nack+requeue for redelivery
                    # (handlers are idempotent under at-least-once delivery).
                    if not message.is_settled:
                        message.nack(requeue=True)
                    return

                # Settle on success
                strategy.on_success(message)

            except AckMessage:
                if not message.is_settled:
                    message.ack()

            except NackMessage as exc:
                if not message.is_settled:
                    message.nack(requeue=exc.requeue)

            except RejectMessage as exc:
                if not message.is_settled:
                    message.reject(requeue=exc.requeue)

            except Exception as exc:
                self._handle_sync_exception(route, message, exc)

        finally:
            _emit_settlement_metric(route, message)
            if debug:
                structlog.contextvars.clear_contextvars()

    async def process_async(
        self,
        route: RouteDefinition,
        message: RabbitMessage,
        publish_fn: Callable[[MessageEnvelope], Awaitable[PublishOutcome]] | None = None,
    ) -> None:
        """Async pipeline — calls await msg.ack_async(), await handler(), await publish().

        Same stages as sync, but async.
        """
        # Filter check — reject before any processing
        if route.filter_fn is not None and not route.filter_fn(message):
            if not message.is_settled:
                await message.nack_async(requeue=False)
            _emit_settlement_metric(route, message)
            return

        # M-P3: only bind contextvars when DEBUG is emitted.
        debug = _stdlib_logger.isEnabledFor(logging.DEBUG)
        if debug:
            structlog.contextvars.bind_contextvars(
                message_id=message.message_id,
                routing_key=message.routing_key,
                queue=route.queue.name,
                handler=getattr(route.handler, "__qualname__", repr(route.handler)),
            )

        try:
            strategy = _ACK_STRATEGIES_ASYNC[route.ack_policy]

            # ACK_FIRST: ack before handler runs
            if strategy.acks_first:
                await message.ack_async()

            try:
                # Resolve parameters and call handler (through the middleware chain)
                result = await self._run_consume_async(route, message)

                # Publish result if needed (Contract 5). M7: skip the
                # REQUEUED_FOR_RETRY sentinel (see the sync path above).
                if (
                    result is not None
                    and result is not REQUEUED_FOR_RETRY
                    and not await self._publish_result_async(route, message, result, publish_fn)
                ):
                    # Result lost — don't ack. Nack+requeue for redelivery
                    # (handlers are idempotent under at-least-once delivery).
                    if not message.is_settled:
                        await message.nack_async(requeue=True)
                    return

                # Settle on success
                await strategy.on_success(message)

            except AckMessage:
                if not message.is_settled:
                    await message.ack_async()

            except NackMessage as exc:
                if not message.is_settled:
                    await message.nack_async(requeue=exc.requeue)

            except RejectMessage as exc:
                if not message.is_settled:
                    await message.reject_async(requeue=exc.requeue)

            except Exception as exc:
                await self._handle_async_exception(route, message, exc)

        finally:
            _emit_settlement_metric(route, message)
            if debug:
                structlog.contextvars.clear_contextvars()

    # ── Internal: middleware composition ─────────────────────────────────

    def _run_consume_sync(self, route: RouteDefinition, message: RabbitMessage) -> Any:
        """Run on_receive hooks, then the consume_scope chain around the handler.

        Middlewares are applied OUTER → INNER: the first item in
        ``route.route_middlewares`` is the outermost wrapper. Each middleware's
        ``consume_scope(call_next, message)`` wraps the next; the innermost
        ``call_next`` deserializes + resolves + invokes the handler.

        H7 — on_receive ordering and exception semantics (READ THIS before
        combining SigningMiddleware/CompressionMiddleware or writing your own
        on_receive-based transform):

        * on_receive hooks run in a FIXED, FLAT pre-pass — entirely BEFORE the
          consume_scope chain is entered. An exception raised here is NEVER
          seen by any middleware's consume_scope (RetryMiddleware included):
          it propagates straight to process_sync's own exception handler,
          which settles the message per the route's AckPolicy using the
          pipeline's default classifier — NOT RetryMiddleware's classifier
          or predicates, and NEVER via RetryMiddleware's delay-queue routing.
          A signing/decompression failure is not retried; it typically
          rejects straight to the DLQ. This is deliberate (an on_receive
          failure means "this delivery is untrustworthy or unreadable," not
          "the handler failed" — retrying doesn't make a bad signature or a
          corrupt payload become valid) and unlikely to change; if you need a
          retry-eligible on_receive-style check, put it in ``consume_scope``
          instead, where it participates in the same chain as everything else.
        * on_receive hooks run in REVERSE registration order — deliberately
          the mirror of publish_scope's OUTER→INNER composition order, so a
          receive-side "undo" (e.g. decompress) always runs relative to a
          publish-side "apply" (e.g. compress) in the mathematically correct
          order regardless of what's paired with what. Concretely: for
          ``middlewares=[A, B]``, publish applies A's transform then B's (A
          outer, B inner); on_receive runs B's hook then A's — the reverse —
          so whichever transform was applied LAST on publish is undone FIRST
          on receive. Before this fix, on_receive ran in the SAME (forward)
          order as publish_scope's apply order, so a receive-side hook always
          ran against a body/metadata state that had already been
          (or not yet been) transformed by the OTHER middleware — never
          matching what that middleware actually needs.
        * This fix does NOT make ``middlewares=[SigningMiddleware,
          CompressionMiddleware]`` order-independent — only ONE relative
          order works: ``middlewares=[CompressionMiddleware,
          SigningMiddleware]`` (compression outer, signing inner). Reason:
          SigningMiddleware's signature covers ``content_encoding`` (H3),
          which is a field CompressionMiddleware's ``publish_scope`` is what
          actually *sets*. If signing runs first (outer), it necessarily
          signs ``content_encoding=None`` (unset at that point) while
          compression sets it to e.g. ``"gzip"`` afterward — the delivered
          message's ``content_encoding`` then never matches what was signed,
          and verification fails unconditionally, independent of the
          on_receive reordering above. Compression outer / signing inner is
          the only order where signing sees the FINAL ``content_encoding``
          it needs to sign correctly. See
          ``tests/unit/core/test_pipeline_middleware.py::TestSigningCompressionComposition``
          for a worked example of both the working and the failing order.
        """
        middlewares = route.route_middlewares
        if not middlewares:
            return self._invoke_handler_sync(route, message)

        for mw in reversed(middlewares):
            mw.on_receive(message)

        # M-P1: cache the composed chain per route — closures are allocated once
        # per route, not per message.
        chain = self._consume_chain_cache.get(id(route))
        if chain is None:

            def call_next(msg: RabbitMessage) -> Any:
                return self._invoke_handler_sync(route, msg)

            for mw in reversed(middlewares):
                nxt = call_next

                def wrapped(msg: RabbitMessage, _mw: Any = mw, _nxt: Any = nxt) -> Any:
                    return _mw.consume_scope(_nxt, msg)

                call_next = wrapped

            chain = call_next
            self._consume_chain_cache[id(route)] = chain

        return chain(message)

    async def _run_consume_async(self, route: RouteDefinition, message: RabbitMessage) -> Any:
        """Async variant of :meth:`_run_consume_sync` — see its docstring
        (H7) for on_receive's ordering and exception-interception semantics,
        which apply identically here."""
        middlewares = route.route_middlewares
        if not middlewares:
            return await self._invoke_handler_async(route, message)

        for mw in reversed(middlewares):
            await mw.on_receive_async(message)

        chain = self._consume_chain_async_cache.get(id(route))
        if chain is None:

            async def call_next(msg: RabbitMessage) -> Any:
                return await self._invoke_handler_async(route, msg)

            for mw in reversed(middlewares):
                nxt = call_next

                async def wrapped(msg: RabbitMessage, _mw: Any = mw, _nxt: Any = nxt) -> Any:
                    return await _mw.consume_scope_async(_nxt, msg)

                call_next = wrapped

            chain = call_next
            self._consume_chain_async_cache[id(route)] = chain

        return await chain(message)

    # ── Internal: handler invocation ─────────────────────────────────────

    def _invoke_handler_sync(self, route: RouteDefinition, message: RabbitMessage) -> Any:
        """Deserialize, resolve params, call handler (sync).

        A ``DependencyScope`` is created whenever the *effective* resolver is
        non-None (explicit OR auto-DI), so generator dependencies are always
        tracked for teardown — including the documented zero-setup marker path.
        The scope wraps BOTH resolution and handler invocation, so a resolution
        failure still tears down any generators opened earlier in the same call.
        """
        # Deserialize body if serializer is available
        body = self._deserialize_body(route, message)

        # Create scope whenever the effective resolver (explicit or auto) is in
        # play — this is the fix for the auto-DI generator-teardown leak.
        resolver = self._effective_resolver(route.handler)
        scope: DependencyScope | None = None
        if resolver is not None and hasattr(resolver, "resolve"):
            scope = DependencyScope()

        # Resolve + invoke under a single try/finally so resolution failures
        # also run generator teardown (generators opened before the failing one).
        try:
            kwargs = self._resolve_params(route, message, body, scope=scope)
            return route.handler(**kwargs)
        finally:
            if scope is not None:
                try:
                    scope.cleanup()
                except Exception as cleanup_exc:
                    logger.error(
                        "DI generator cleanup raised an exception — possible resource leak: %s",
                        cleanup_exc,
                        exc_info=True,
                    )

    async def _invoke_handler_async(self, route: RouteDefinition, message: RabbitMessage) -> Any:
        """Deserialize, resolve params, call handler (async)."""
        body = await self._deserialize_body_async(route, message)

        resolver = self._effective_resolver(route.handler)
        scope: DependencyScope | None = None
        if resolver is not None and hasattr(resolver, "resolve_async"):
            scope = DependencyScope()

        try:
            kwargs = await self._resolve_params_async(route, message, body, scope=scope)
            result = route.handler(**kwargs)
            # P5: cached async detection — avoids per-message hasattr(result, "__await__").
            is_async = self._is_async_handler_cache.get(route.handler)
            if is_async is None:
                import inspect as _inspect
                is_async = _inspect.iscoroutinefunction(route.handler)
                self._is_async_handler_cache[route.handler] = is_async
            if is_async:
                result = await result
            return result
        finally:
            if scope is not None:
                try:
                    await scope.cleanup_async()
                except Exception as cleanup_exc:
                    logger.error(
                        "DI generator cleanup raised an exception — possible resource leak: %s",
                        cleanup_exc,
                        exc_info=True,
                    )

    def _deserialize_body(self, route: RouteDefinition, message: RabbitMessage) -> Any:
        """Deserialize message body using the route's serializer."""
        serializer = route.serializer_override or self._serializer
        if serializer is None:
            return message.body
        # P3: cached hasattr check — avoids a per-message attribute lookup.
        can_decode = self._has_decode_cache.get(serializer)
        if can_decode is None:
            can_decode = hasattr(serializer, "decode")
            self._has_decode_cache[serializer] = can_decode
        if can_decode:
            target_type = self._get_body_type(route)
            if target_type is not None and target_type is not bytes:
                return serializer.decode(message.body, target_type)
        return message.body

    async def _deserialize_body_async(self, route: RouteDefinition, message: RabbitMessage) -> Any:
        """Async body deserialization (M10).

        Identical to :meth:`_deserialize_body`, except a large body is decoded
        in a worker thread via ``asyncio.to_thread`` so a multi-MB
        JSON/msgspec/pydantic parse doesn't block the event loop — which would
        otherwise stall heartbeats, publisher confirms, and every other
        consumer sharing the loop. Small bodies decode inline (the thread hop
        costs more than the parse).
        """
        serializer = route.serializer_override or self._serializer
        if serializer is None:
            return message.body
        can_decode = self._has_decode_cache.get(serializer)
        if can_decode is None:
            can_decode = hasattr(serializer, "decode")
            self._has_decode_cache[serializer] = can_decode
        if can_decode:
            target_type = self._get_body_type(route)
            if target_type is not None and target_type is not bytes:
                if len(message.body) >= _DECODE_OFFLOAD_THRESHOLD_BYTES:
                    import asyncio

                    return await asyncio.to_thread(serializer.decode, message.body, target_type)
                return serializer.decode(message.body, target_type)
        return message.body

    def _get_body_type(self, route: RouteDefinition) -> type | None:
        """Get the body parameter type from the handler signature (cached per handler)."""
        handler = route.handler
        if handler in self._body_type_cache:
            return self._body_type_cache[handler]
        body_type = self._compute_body_type(route)
        self._body_type_cache[handler] = body_type
        return body_type

    def _compute_body_type(self, route: RouteDefinition) -> type | None:
        """Resolve the body parameter type. Returns None if none or if it is bytes.

        Uses ``typing.get_type_hints()`` so that string annotations produced by
        ``from __future__ import annotations`` are resolved to their real types
        before being handed to the serializer.  Falls back to the raw
        ``inspect.Parameter.annotation`` value when ``get_type_hints()`` cannot
        resolve the annotation (e.g. forward references that are not yet defined).
        """
        import inspect
        import typing

        try:
            hints = typing.get_type_hints(route.handler, include_extras=True)
        except Exception:
            hints = {}

        sig = inspect.signature(route.handler)
        for param_name, param in sig.parameters.items():
            # Prefer the resolved hint; fall back to the raw annotation.
            ann = hints.get(param_name, param.annotation)
            if ann is inspect.Parameter.empty:
                continue
            # Skip RabbitMessage type
            if is_rabbit_message_annotation(ann):
                continue
            # Skip Annotated types (DI marker)
            origin = getattr(ann, "__metadata__", None)
            if origin is not None:
                continue
            # First non-special parameter is the body type
            return ann  # type: ignore[no-any-return]
        return None

    def _resolve_params(
        self,
        route: RouteDefinition,
        message: RabbitMessage,
        body: Any,
        scope: Any | None = None,
    ) -> dict[str, Any]:
        """Resolve handler parameters.

        Uses DI resolver if available, otherwise falls back to simple
        body + message injection.
        """
        resolver = self._effective_resolver(route.handler)
        if resolver is not None and hasattr(resolver, "resolve"):
            return resolver.resolve(route.handler, message, self._context_repo, body, scope=scope)  # type: ignore[no-any-return]

        # P4: precomputed binding plan — avoids per-message inspect.signature iteration,
        # is_rabbit_message_annotation string checks, and param.default lookups.
        # The plan is a list of (param_name, action) where action is
        # "message", "body", or "skip" (use default). Computed once per handler.
        plan = self._binding_plan_cache.get(route.handler)
        if plan is None:
            import inspect
            sig = self._sig_cache.get(route.handler)
            if sig is None:
                sig = inspect.signature(route.handler)
                self._sig_cache[route.handler] = sig
            plan = []
            body_injected = False
            for param_name, param in sig.parameters.items():
                # Skip *args and **kwargs — they can't be passed via **kwargs
                if param.kind in (inspect.Parameter.VAR_POSITIONAL, inspect.Parameter.VAR_KEYWORD):
                    plan.append((param_name, "skip"))
                    continue
                ann = param.annotation
                if is_rabbit_message_annotation(ann):
                    plan.append((param_name, "message"))
                elif not body_injected:
                    plan.append((param_name, "body"))
                    body_injected = True
                elif param.default is not inspect.Parameter.empty:
                    plan.append((param_name, "skip"))
                else:
                    plan.append((param_name, "message"))
            self._binding_plan_cache[route.handler] = plan

        # Execute the plan — a tight loop with no per-message reflection.
        kwargs: dict[str, Any] = {}
        for param_name, action in plan:
            if action == "message":
                kwargs[param_name] = message
            elif action == "body":
                kwargs[param_name] = body
            # "skip" → use default (omit from kwargs)
        return kwargs

    async def _resolve_params_async(
        self,
        route: RouteDefinition,
        message: RabbitMessage,
        body: Any,
        scope: Any | None = None,
    ) -> dict[str, Any]:
        """Resolve handler parameters (async variant).

        Uses async DI resolver if available, otherwise falls back to sync resolve.
        """
        resolver = self._effective_resolver(route.handler)
        if resolver is not None and hasattr(resolver, "resolve_async"):
            return await resolver.resolve_async(  # type: ignore[no-any-return]
                route.handler, message, self._context_repo, body, scope=scope
            )
        # Fall back to sync resolve (fast path for marker-free handlers)
        return self._resolve_params(route, message, body, scope=scope)

    def _effective_resolver(self, handler: Any) -> Any | None:
        """Return the resolver to use for a handler.

        Explicit `di_resolver` always wins. Otherwise, handlers that use DI markers
        (Depends/Header/Path/Context) get a lazily-created default DIResolver, so the
        documented markers work without the caller wiring one. Marker-free handlers
        return None → the fast body/message fallback, unchanged.
        """
        if self._di_resolver is not None:
            return self._di_resolver
        if not self._handler_needs_di(handler):
            return None
        if self._auto_resolver is None:
            self._auto_resolver = DIResolver()
        return self._auto_resolver

    def _handler_needs_di(self, handler: Any) -> bool:
        """True if any parameter is Annotated with a DI marker. Cached per handler.

        L11: uses the SAME hint-resolution strength as ``DIResolver`` itself
        (``get_type_hints_with_fallback`` — includes the closure-``localns``
        retry) so this detector and the resolver it gates never diverge. A
        weaker, 2-attempt version used to live here; a closure-scoped
        ``Depends(...)`` annotation could resolve fine for ``DIResolver`` but
        still be mis-detected as "no DI needed" by this method, silently
        binding the marked parameter to the message body instead.
        """
        cached = self._needs_di_cache.get(handler)
        if cached is not None:
            return cached

        from rabbitkit.di.context import Context, Header, Path
        from rabbitkit.di.depends import Depends
        from rabbitkit.di.resolver import get_type_hints_with_fallback

        hints = get_type_hints_with_fallback(handler)

        markers = (Depends, Header, Path, Context)
        needs = any(any(isinstance(m, markers) for m in getattr(ann, "__metadata__", ())) for ann in hints.values())
        self._needs_di_cache[handler] = needs
        return needs

    # ── Internal: result publishing ──────────────────────────────────────

    def _compose_publish_sync(
        self,
        route: RouteDefinition,
        publish_fn: Callable[[MessageEnvelope], PublishOutcome],
    ) -> Callable[[MessageEnvelope, Callable[[MessageEnvelope], PublishOutcome]], Any]:
        """Compose this route's ``publish_scope`` middlewares into a reusable chain.

        So a route that carries e.g. a signing/tracing middleware applies it to
        the results it publishes. (Standalone producer publishes via
        ``broker.publish`` are not route-scoped and apply publish middlewares
        manually — see docs.)

        The composed chain is cached per route (keyed by ``id(route)``) — the
        middleware list is fixed after registration, so rebuild once per route
        instead of allocating N closures per message (mirrors the consume cache).

        L-1: ``publish_fn`` is NOT captured in the cached closure — it is threaded
        through at invocation time as the second argument, so a later call with a
        different ``publish_fn`` actually uses the new one (previously the first
        ``publish_fn`` was silently captured and reused forever).
        """
        cached = self._publish_chain_cache.get(id(route))
        if cached is not None:
            return cached

        # The innermost shim defers to ``publish_fn`` supplied at call time.
        def leaf(env: MessageEnvelope, fn: Callable[[MessageEnvelope], PublishOutcome]) -> Any:
            return fn(env)

        chain: Callable[[MessageEnvelope, Callable[[MessageEnvelope], PublishOutcome]], Any] = leaf
        for mw in reversed(route.route_middlewares):
            nxt = chain

            def wrapped(
                env: MessageEnvelope,
                fn: Callable[[MessageEnvelope], PublishOutcome],
                _mw: Any = mw,
                _nxt: Any = nxt,
            ) -> Any:
                # Bind the current publish_fn into the call_next shim so the
                # middleware's publish_scope(call_next, env) signature is unchanged.
                return _mw.publish_scope(lambda e: _nxt(e, fn), env)

            chain = wrapped
        self._publish_chain_cache[id(route)] = chain
        return chain

    def _publish_result_sync(
        self,
        route: RouteDefinition,
        message: RabbitMessage,
        result: Any,
        publish_fn: Callable[[MessageEnvelope], PublishOutcome] | None,
    ) -> bool:
        """Publish handler result (Contract 5 precedence).

        Returns False only when a publish was attempted and failed, so the
        caller can avoid acking a message whose result was lost — the
        caller nacks with ``requeue=True`` instead (see ``process_sync``).

        L1: a nack+requeue here re-runs the handler from scratch on
        redelivery, including any side effects it already performed —
        this is only safe if handlers are idempotent (the same baseline
        assumption at-least-once delivery already requires everywhere
        else; see ``docs/rabbitmq-retry-architecture.md``). If
        ``message.redelivered`` is already ``True``, this is not the
        first time this exact message has hit a failing result publish —
        logged at ERROR (vs. WARNING for a first attempt) so a sustained
        publish outage that would otherwise hot-loop silently is loud and
        alertable via log-based monitoring.
        """
        if publish_fn is None:
            return True

        envelope = self._build_result_envelope(route, message, result)
        if envelope is None:
            return True

        outcome = self._compose_publish_sync(route, publish_fn)(envelope, publish_fn)
        if not outcome.ok:
            _log_result_publish_failure(message, outcome)
            return False
        return True

    def _compose_publish_async(
        self,
        route: RouteDefinition,
        publish_fn: Callable[[MessageEnvelope], Awaitable[PublishOutcome]],
    ) -> Callable[[MessageEnvelope, Callable[[MessageEnvelope], Awaitable[PublishOutcome]]], Awaitable[Any]]:
        """Async variant of :meth:`_compose_publish_sync`.

        The composed chain is cached per route (keyed by ``id(route)``) — see
        :meth:`_compose_publish_sync` for the rationale.

        L-1: ``publish_fn`` is NOT captured in the cached closure — it is threaded
        through at invocation time as the second argument.
        """
        cached = self._publish_chain_async_cache.get(id(route))
        if cached is not None:
            return cached

        async def leaf(
            env: MessageEnvelope,
            fn: Callable[[MessageEnvelope], Awaitable[PublishOutcome]],
        ) -> Any:
            return await fn(env)

        chain: Callable[[MessageEnvelope, Callable[[MessageEnvelope], Awaitable[PublishOutcome]]], Awaitable[Any]] = (
            leaf
        )
        for mw in reversed(route.route_middlewares):
            nxt = chain

            async def wrapped(
                env: MessageEnvelope,
                fn: Callable[[MessageEnvelope], Awaitable[PublishOutcome]],
                _mw: Any = mw,
                _nxt: Any = nxt,
            ) -> Any:
                return await _mw.publish_scope_async(lambda e: _nxt(e, fn), env)

            chain = wrapped
        self._publish_chain_async_cache[id(route)] = chain
        return chain

    async def _publish_result_async(
        self,
        route: RouteDefinition,
        message: RabbitMessage,
        result: Any,
        publish_fn: Callable[[MessageEnvelope], Awaitable[PublishOutcome]] | None,
    ) -> bool:
        """Publish handler result (async, Contract 5 precedence).

        Returns False only when a publish was attempted and failed. See
        :meth:`_publish_result_sync` (L1) for the redelivery-escalation
        rationale — identical here.
        """
        if publish_fn is None:
            return True

        envelope = self._build_result_envelope(route, message, result)
        if envelope is None:
            return True

        outcome = await self._compose_publish_async(route, publish_fn)(envelope, publish_fn)
        if not outcome.ok:
            _log_result_publish_failure(message, outcome)
            return False
        return True

    def _build_result_envelope(
        self,
        route: RouteDefinition,
        message: RabbitMessage,
        result: Any,
    ) -> MessageEnvelope | None:
        """Build MessageEnvelope from handler result.

        Contract 5 precedence:
        1. None return → no publish
        2. reply_to → RPC reply (takes precedence)
        3. result_publisher → publish to configured exchange/routing_key
        4. Both → reply_to wins
        """
        if result is None:
            return None

        user_envelope = result if isinstance(result, MessageEnvelope) else None
        body = self._serialize_result(route, result)

        # Determine destination (Contract 5)
        if message.reply_to:
            # RPC reply takes precedence
            if user_envelope is not None:
                # Preserve user-provided fields (headers, message_id,
                # content_type, priority, expiration, ...); only the
                # precedence-driven destination is merged in.
                return dataclasses.replace(
                    user_envelope,
                    routing_key=message.reply_to,
                    exchange="",
                    correlation_id=message.correlation_id,
                )
            return MessageEnvelope(
                routing_key=message.reply_to,
                body=body,
                exchange="",
                correlation_id=message.correlation_id,
            )

        if route.result_publisher is not None:
            exchange_name = route.result_publisher.resolve_exchange_name()
            if user_envelope is not None:
                # Override only exchange/routing_key; keep user fields.
                return dataclasses.replace(
                    user_envelope,
                    routing_key=route.result_publisher.routing_key,
                    exchange=exchange_name,
                )
            return MessageEnvelope(
                routing_key=route.result_publisher.routing_key,
                body=body,
                exchange=exchange_name,
            )

        if user_envelope is not None:
            logger.warning(
                "handler returned a MessageEnvelope but route has no result_publisher"
                " and message has no reply_to; result dropped"
            )
        return None

    def _serialize_result(self, route: RouteDefinition, result: Any) -> bytes:
        """Serialize handler return value to bytes."""
        if isinstance(result, bytes):
            return result
        if isinstance(result, str):
            return result.encode("utf-8")
        if isinstance(result, MessageEnvelope):
            return result.body

        serializer = route.serializer_override or self._serializer
        if serializer is not None and hasattr(serializer, "encode"):
            return serializer.encode(result)

        # Fallback: JSON encode
        import json

        return json.dumps(result).encode("utf-8")

    # ── Internal: exception handling ─────────────────────────────────────

    def _handle_sync_exception(
        self,
        route: RouteDefinition,
        message: RabbitMessage,
        exc: Exception,
    ) -> None:
        """Handle exception in sync pipeline per AckPolicy (Contract 1)."""
        try:
            self._handle_sync_exception_inner(route, message, exc)
        except Exception as settle_exc:
            # The settlement attempt itself failed because the channel or
            # connection died (SIGTERM drain, broker restart, network cut).
            # Nothing further can be settled on a dead channel and the broker
            # will redeliver the unacked message — warn instead of letting a
            # secondary exception escape as a full ERROR traceback.
            if not _is_channel_gone(settle_exc):
                raise
            self._settlement_loss_warner.warn(type(settle_exc).__name__)

    def _handle_sync_exception_inner(
        self,
        route: RouteDefinition,
        message: RabbitMessage,
        exc: Exception,
    ) -> None:
        if message.is_settled:
            # Already settled (e.g., MANUAL mode handler settled then raised)
            logger.warning("Exception after settlement: %s", exc)
            return

        # RetryMiddleware tags exhausted/permanent failures as terminal. Dead-letter
        # them (reject → source-queue DLX → DLQ) rather than re-classifying: an
        # *exhausted transient* error would otherwise be re-classified TRANSIENT and
        # nack(requeue=True)'d straight back into a hot loop, never reaching the DLQ.
        # MANUAL is excluded — retry is incompatible with MANUAL (handler owns ack).
        if getattr(exc, "_rabbitkit_terminal", False) and route.ack_policy != AckPolicy.MANUAL:
            message.reject(requeue=False)
            return

        # M6: 2-strike cap on the transient hot-loop. A transient error on a
        # message the broker has already redelivered escalates to the DLQ
        # instead of an unbounded nack-requeue. Only AUTO requeues transients;
        # opt-in via ConsumerConfig.reject_transient_on_redelivery.
        if (
            self._reject_transient_on_redelivery
            and message.redelivered
            and route.ack_policy == AckPolicy.AUTO
            and classify_error(exc).severity == ErrorSeverity.TRANSIENT
        ):
            logger.warning(
                "Transient error on an already-redelivered message; rejecting to DLQ "
                "instead of requeuing again (reject_transient_on_redelivery)",
                exc_info=True,
            )
            message.reject(requeue=False)
            return

        _ACK_STRATEGIES[route.ack_policy].on_error(message, exc)

    async def _handle_async_exception(
        self,
        route: RouteDefinition,
        message: RabbitMessage,
        exc: Exception,
    ) -> None:
        """Handle exception in async pipeline per AckPolicy (Contract 1)."""
        try:
            await self._handle_async_exception_inner(route, message, exc)
        except Exception as settle_exc:
            # See _handle_sync_exception: a dead channel means nothing can be
            # settled and the broker redelivers — warn, don't raise.
            if not _is_channel_gone(settle_exc):
                raise
            self._settlement_loss_warner.warn(type(settle_exc).__name__)

    async def _handle_async_exception_inner(
        self,
        route: RouteDefinition,
        message: RabbitMessage,
        exc: Exception,
    ) -> None:
        if message.is_settled:
            logger.warning("Exception after settlement: %s", exc)
            return

        # See _handle_sync_exception: terminal (exhausted/permanent) failures
        # dead-letter directly instead of being re-classified into a hot loop.
        if getattr(exc, "_rabbitkit_terminal", False) and route.ack_policy != AckPolicy.MANUAL:
            await message.reject_async(requeue=False)
            return

        # M6: 2-strike cap on the transient hot-loop (see _handle_sync_exception).
        if (
            self._reject_transient_on_redelivery
            and message.redelivered
            and route.ack_policy == AckPolicy.AUTO
            and classify_error(exc).severity == ErrorSeverity.TRANSIENT
        ):
            logger.warning(
                "Transient error on an already-redelivered message; rejecting to DLQ "
                "instead of requeuing again (reject_transient_on_redelivery)",
                exc_info=True,
            )
            await message.reject_async(requeue=False)
            return

        await _ACK_STRATEGIES_ASYNC[route.ack_policy].on_error(message, exc)
