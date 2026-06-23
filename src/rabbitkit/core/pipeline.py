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

from collections.abc import Awaitable, Callable
from typing import Any

import structlog

from rabbitkit.core.errors import classify_error
from rabbitkit.core.message import AckMessage, NackMessage, RabbitMessage, RejectMessage
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.types import AckPolicy, ErrorSeverity, MessageEnvelope, PublishOutcome

logger = structlog.stdlib.get_logger(__name__)


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
        serializer: Any | None = None,
        di_resolver: Any | None = None,
        context_repo: Any | None = None,
    ) -> None:
        self._serializer = serializer
        self._di_resolver = di_resolver
        self._context_repo = context_repo
        # Per-handler cache of the body parameter type (static per route) so the
        # hot path avoids inspect.signature() on every message.
        self._body_type_cache: dict[Any, type | None] = {}

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

        structlog.contextvars.bind_contextvars(
            message_id=message.message_id,
            routing_key=message.routing_key,
            queue=route.queue.name,
            handler=getattr(route.handler, "__qualname__", repr(route.handler)),
        )

        try:
            # ACK_FIRST: ack before handler runs
            if route.ack_policy == AckPolicy.ACK_FIRST:
                message.ack()

            try:
                # Resolve parameters and call handler (through the middleware chain)
                result = self._run_consume_sync(route, message)

                # Publish result if needed (Contract 5)
                if result is not None and not self._publish_result_sync(
                    route, message, result, publish_fn
                ):
                    # Result lost — don't ack. Nack+requeue for redelivery
                    # (handlers are idempotent under at-least-once delivery).
                    if not message.is_settled:
                        message.nack(requeue=True)
                    return

                # Settle on success
                if not message.is_settled:
                    message.ack()

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

        structlog.contextvars.bind_contextvars(
            message_id=message.message_id,
            routing_key=message.routing_key,
            queue=route.queue.name,
            handler=getattr(route.handler, "__qualname__", repr(route.handler)),
        )

        try:
            # ACK_FIRST: ack before handler runs
            if route.ack_policy == AckPolicy.ACK_FIRST:
                await message.ack_async()

            try:
                # Resolve parameters and call handler (through the middleware chain)
                result = await self._run_consume_async(route, message)

                # Publish result if needed (Contract 5)
                if result is not None and not await self._publish_result_async(
                    route, message, result, publish_fn
                ):
                    # Result lost — don't ack. Nack+requeue for redelivery
                    # (handlers are idempotent under at-least-once delivery).
                    if not message.is_settled:
                        await message.nack_async(requeue=True)
                    return

                # Settle on success
                if not message.is_settled:
                    await message.ack_async()

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

        def call_next(msg: RabbitMessage) -> Any:
            return self._invoke_handler_sync(route, msg)

        for mw in reversed(middlewares):
            nxt = call_next

            def wrapped(msg: RabbitMessage, _mw: Any = mw, _nxt: Any = nxt) -> Any:
                return _mw.consume_scope(_nxt, msg)

            call_next = wrapped

        return call_next(message)

    async def _run_consume_async(self, route: RouteDefinition, message: RabbitMessage) -> Any:
        """Async variant of :meth:`_run_consume_sync`."""
        middlewares = route.route_middlewares
        if not middlewares:
            return await self._invoke_handler_async(route, message)

        for mw in middlewares:
            await mw.on_receive_async(message)

        async def call_next(msg: RabbitMessage) -> Any:
            return await self._invoke_handler_async(route, msg)

        for mw in reversed(middlewares):
            nxt = call_next

            async def wrapped(msg: RabbitMessage, _mw: Any = mw, _nxt: Any = nxt) -> Any:
                return await _mw.consume_scope_async(_nxt, msg)

            call_next = wrapped

        return await call_next(message)

    # ── Internal: handler invocation ─────────────────────────────────────

    def _invoke_handler_sync(self, route: RouteDefinition, message: RabbitMessage) -> Any:
        """Deserialize, resolve params, call handler (sync)."""
        from rabbitkit.di.resolver import DependencyScope

        # Deserialize body if serializer is available
        body = self._deserialize_body(route, message)

        # Create scope for generator cleanup
        scope: DependencyScope | None = None
        if self._di_resolver is not None and hasattr(self._di_resolver, "resolve"):
            scope = DependencyScope()

        # Resolve handler parameters
        kwargs = self._resolve_params(route, message, body, scope=scope)

        # Call handler with cleanup guarantee
        try:
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
        from rabbitkit.di.resolver import DependencyScope

        body = self._deserialize_body(route, message)

        # Create scope for generator cleanup
        scope: DependencyScope | None = None
        if self._di_resolver is not None and hasattr(self._di_resolver, "resolve_async"):
            scope = DependencyScope()

        kwargs = await self._resolve_params_async(route, message, body, scope=scope)

        try:
            result = route.handler(**kwargs)
            # If handler is async, await it
            if hasattr(result, "__await__"):
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
        if serializer is not None and hasattr(serializer, "decode"):
            # Get target type from handler signature if available
            target_type = self._get_body_type(route)
            if target_type is not None and target_type is not bytes:
                decoded = serializer.decode(message.body, target_type)
                # Auto-validate if target is a Pydantic model and decoded is a dict
                if (
                    isinstance(decoded, dict)
                    and target_type is not dict
                    and hasattr(target_type, "model_validate")
                ):
                    return target_type.model_validate(decoded)
                return decoded
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
        """Resolve the body parameter type. Returns None if none or if it is bytes."""
        import inspect

        sig = inspect.signature(route.handler)
        for _, param in sig.parameters.items():
            ann = param.annotation
            if ann is inspect.Parameter.empty:
                continue
            # Skip RabbitMessage type
            if ann is RabbitMessage:
                continue
            # Skip Annotated types (DI markers)
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
        if self._di_resolver is not None and hasattr(self._di_resolver, "resolve"):
            return self._di_resolver.resolve(route.handler, message, self._context_repo, body, scope=scope)  # type: ignore[no-any-return]

        # Simple fallback: inject body and/or message based on signature
        import inspect

        sig = inspect.signature(route.handler)
        kwargs: dict[str, Any] = {}

        body_injected = False
        for param_name, param in sig.parameters.items():
            ann = param.annotation
            if ann is RabbitMessage:
                kwargs[param_name] = message
            elif not body_injected:
                kwargs[param_name] = body
                body_injected = True
            elif param.default is not inspect.Parameter.empty:
                continue  # use default
            else:
                kwargs[param_name] = message  # fallback

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
        if self._di_resolver is not None and hasattr(self._di_resolver, "resolve_async"):
            return await self._di_resolver.resolve_async(  # type: ignore[no-any-return]
                route.handler, message, self._context_repo, body, scope=scope
            )
        # Fall back to sync resolve
        return self._resolve_params(route, message, body, scope=scope)

    # ── Internal: result publishing ──────────────────────────────────────

    def _compose_publish_sync(
        self,
        route: RouteDefinition,
        publish_fn: Callable[[MessageEnvelope], PublishOutcome],
    ) -> Callable[[MessageEnvelope], Any]:
        """Compose this route's ``publish_scope`` middlewares around publish_fn.

        So a route that carries e.g. a signing/tracing middleware applies it to
        the results it publishes. (Standalone producer publishes via
        ``broker.publish`` are not route-scoped and apply publish middlewares
        manually — see docs.)
        """
        chain: Callable[[MessageEnvelope], Any] = publish_fn
        for mw in reversed(route.route_middlewares):
            nxt = chain

            def wrapped(env: MessageEnvelope, _mw: Any = mw, _nxt: Any = nxt) -> Any:
                return _mw.publish_scope(_nxt, env)

            chain = wrapped
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

        outcome = self._compose_publish_sync(route, publish_fn)(envelope)
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
    ) -> Callable[[MessageEnvelope], Awaitable[Any]]:
        """Async variant of :meth:`_compose_publish_sync`."""
        chain: Callable[[MessageEnvelope], Awaitable[Any]] = publish_fn
        for mw in reversed(route.route_middlewares):
            nxt = chain

            async def wrapped(env: MessageEnvelope, _mw: Any = mw, _nxt: Any = nxt) -> Any:
                return await _mw.publish_scope_async(_nxt, env)

            chain = wrapped
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

        outcome = await self._compose_publish_async(route, publish_fn)(envelope)
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

        # Serialize result
        body = self._serialize_result(route, result)

        # Determine destination (Contract 5)
        if message.reply_to:
            # RPC reply takes precedence
            return MessageEnvelope(
                routing_key=message.reply_to,
                body=body,
                exchange="",
                correlation_id=message.correlation_id,
            )

        if route.result_publisher is not None:
            exchange_name = route.result_publisher.resolve_exchange_name()
            return MessageEnvelope(
                routing_key=route.result_publisher.routing_key,
                body=body,
                exchange=exchange_name,
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
            return serializer.encode(result)  # type: ignore[no-any-return]

        # Fallback: JSON encode
        import json

        return json.dumps(result, default=str).encode("utf-8")

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

        if route.ack_policy == AckPolicy.MANUAL:
            # MANUAL: handler owns settlement, don't interfere
            logger.error("Unhandled exception in MANUAL mode handler: %s", exc)
            raise

        if route.ack_policy == AckPolicy.NACK_ON_ERROR:
            message.nack(requeue=False)
            return

        # AUTO policy: classify error
        classified = classify_error(exc)
        if classified.severity == ErrorSeverity.TRANSIENT:
            message.nack(requeue=True)
        else:
            message.reject(requeue=False)

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

        if route.ack_policy == AckPolicy.MANUAL:
            logger.error("Unhandled exception in MANUAL mode handler: %s", exc)
            raise

        if route.ack_policy == AckPolicy.NACK_ON_ERROR:
            await message.nack_async(requeue=False)
            return

        # AUTO policy: classify error
        classified = classify_error(exc)
        if classified.severity == ErrorSeverity.TRANSIENT:
            await message.nack_async(requeue=True)
        else:
            await message.reject_async(requeue=False)
