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
from collections.abc import Awaitable, Callable
from typing import Any, Protocol

import structlog

from rabbitkit.core.errors import classify_error
from rabbitkit.core.message import AckMessage, NackMessage, RabbitMessage, RejectMessage, is_rabbit_message_annotation
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.types import AckPolicy, AckStrategy, ErrorSeverity, MessageEnvelope, PublishOutcome
from rabbitkit.di.context import ContextRepo
from rabbitkit.di.resolver import DependencyScope, DIResolver
from rabbitkit.serialization.base import Serializer

logger = structlog.stdlib.get_logger(__name__)
_stdlib_logger = logging.getLogger(__name__)


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
    """MANUAL: handler owns settlement; pipeline re-raises unhandled errors."""

    acks_first: bool = False

    def on_success(self, msg: RabbitMessage) -> None:
        if not msg.is_settled:
            msg.ack()

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
    acks_first: bool = False

    async def on_success(self, msg: RabbitMessage) -> None:
        if not msg.is_settled:
            await msg.ack_async()

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
    ) -> None:
        self._serializer = serializer
        self._di_resolver = di_resolver
        self._context_repo = context_repo
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

                # Publish result if needed (Contract 5)
                if result is not None and not self._publish_result_sync(route, message, result, publish_fn):
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

                # Publish result if needed (Contract 5)
                if result is not None and not await self._publish_result_async(route, message, result, publish_fn):
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
            if debug:
                structlog.contextvars.clear_contextvars()

    # ── Internal: middleware composition ─────────────────────────────────

    def _run_consume_sync(self, route: RouteDefinition, message: RabbitMessage) -> Any:
        """Run on_receive hooks, then the consume_scope chain around the handler.

        Middlewares are applied OUTER → INNER: the first item in
        ``route.route_middlewares`` is the outermost wrapper. Each middleware's
        ``consume_scope(call_next, message)`` wraps the next; the innermost
        ``call_next`` deserializes + resolves + invokes the handler.
        """
        middlewares = route.route_middlewares
        if not middlewares:
            return self._invoke_handler_sync(route, message)

        for mw in middlewares:
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
        """Async variant of :meth:`_run_consume_sync`."""
        middlewares = route.route_middlewares
        if not middlewares:
            return await self._invoke_handler_async(route, message)

        for mw in middlewares:
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
        body = self._deserialize_body(route, message)

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
                import asyncio as _aio
                is_async = _aio.iscoroutinefunction(route.handler)
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
        """True if any parameter is Annotated with a DI marker. Cached per handler."""
        cached = self._needs_di_cache.get(handler)
        if cached is not None:
            return cached

        import inspect
        import typing

        try:
            hints = typing.get_type_hints(handler, include_extras=True)
        except Exception:
            try:
                hints = inspect.get_annotations(handler, eval_str=True)
            except Exception:
                hints = getattr(handler, "__annotations__", {})

        from rabbitkit.di.context import Context, Header, Path
        from rabbitkit.di.depends import Depends

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
        caller can avoid acking a message whose result was lost.
        """
        if publish_fn is None:
            return True

        envelope = self._build_result_envelope(route, message, result)
        if envelope is None:
            return True

        outcome = self._compose_publish_sync(route, publish_fn)(envelope, publish_fn)
        if not outcome.ok:
            logger.warning(
                "Result publish failed: status=%s, exchange=%s, routing_key=%s",
                outcome.status,
                outcome.exchange,
                outcome.routing_key,
            )
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

        Returns False only when a publish was attempted and failed.
        """
        if publish_fn is None:
            return True

        envelope = self._build_result_envelope(route, message, result)
        if envelope is None:
            return True

        outcome = await self._compose_publish_async(route, publish_fn)(envelope, publish_fn)
        if not outcome.ok:
            logger.warning(
                "Result publish failed: status=%s, exchange=%s, routing_key=%s",
                outcome.status,
                outcome.exchange,
                outcome.routing_key,
            )
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

        _ACK_STRATEGIES[route.ack_policy].on_error(message, exc)

    async def _handle_async_exception(
        self,
        route: RouteDefinition,
        message: RabbitMessage,
        exc: Exception,
    ) -> None:
        """Handle exception in async pipeline per AckPolicy (Contract 1)."""
        if message.is_settled:
            logger.warning("Exception after settlement: %s", exc)
            return

        # See _handle_sync_exception: terminal (exhausted/permanent) failures
        # dead-letter directly instead of being re-classified into a hot loop.
        if getattr(exc, "_rabbitkit_terminal", False) and route.ack_policy != AckPolicy.MANUAL:
            await message.reject_async(requeue=False)
            return

        await _ACK_STRATEGIES_ASYNC[route.ack_policy].on_error(message, exc)
