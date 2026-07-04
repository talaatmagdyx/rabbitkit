"""OTelTracingMiddleware — native OpenTelemetry tracing (no obskit needed).

Speaks ``opentelemetry-api`` directly so publish→consume trace propagation
works for anyone, using W3C ``traceparent``/``tracestate`` headers over AMQP.
Install with the optional extra::

    pip install rabbitkit[otel]

Lazy/no-op: if ``opentelemetry`` is not importable, every span is a
passthrough — and the middleware warns ONCE at construction (a caller who
adds it is opting into tracing; silently no-oping forever reads as
"nothing to trace yet" instead of "tracing was never active").

Relationship to :class:`~rabbitkit.middleware.tracing.TracedConsumerMiddleware`
(the Lucidya-internal ``obskit`` integration): pick ONE. Stacking both on a
route double-instruments every message.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import replace
from typing import Any

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.base import BaseMiddleware

logger = logging.getLogger(__name__)


def _get_otel() -> Any:
    """Lazy import of the ``opentelemetry`` API — returns module pair or ``None``."""
    try:
        from opentelemetry import propagate, trace

        return trace, propagate
    except ImportError:
        return None


class OTelTracingMiddleware(BaseMiddleware):
    """Wrap handler execution and publishes in standard OpenTelemetry spans.

    - Consume: extracts W3C trace context from message headers and starts a
      ``CONSUMER``-kind span as its child. Handler exceptions are recorded on
      the span (status ``ERROR``) and re-raised.
    - Publish: starts a ``PRODUCER``-kind span and injects the current trace
      context into a COPY of the envelope's headers (envelopes are frozen).

    Span names follow the OTel messaging convention: ``{destination} receive``
    / ``{destination} send``.
    """

    def __init__(self, service_name: str = "rabbitkit") -> None:
        self._service_name = service_name
        otel = _get_otel()
        if otel is None:
            self._trace: Any = None
            self._propagate: Any = None
            self._tracer: Any = None
            logger.warning(
                "OTelTracingMiddleware(service_name=%r) added but opentelemetry is not "
                "installed -- every consume/publish span will be a silent no-op. "
                "Install with `pip install rabbitkit[otel]`, or remove this middleware.",
                service_name,
            )
        else:
            self._trace, self._propagate = otel
            self._tracer = self._trace.get_tracer("rabbitkit", instrumenting_library_version="1")

    @property
    def is_available(self) -> bool:
        """True if the opentelemetry API is importable."""
        return self._tracer is not None

    # ── Internal helpers ──────────────────────────────────────────────────

    @staticmethod
    def _str_carrier(headers: dict[str, Any]) -> dict[str, str]:
        """AMQP headers may hold non-str values; propagators want str→str."""
        return {k: v for k, v in headers.items() if isinstance(v, str)}

    def _consume_attributes(self, message: RabbitMessage) -> dict[str, str]:
        attrs: dict[str, str] = {
            "messaging.system": "rabbitmq",
            "messaging.operation": "receive",
        }
        if message.routing_key:
            attrs["messaging.rabbitmq.destination.routing_key"] = message.routing_key
        queue = message.headers.get("x-rabbitkit-original-queue", "")
        if queue:
            attrs["messaging.destination.name"] = str(queue)
        if message.message_id:
            attrs["messaging.message.id"] = message.message_id
        if message.correlation_id:
            attrs["messaging.message.conversation_id"] = message.correlation_id
        retry_count = message.headers.get("x-rabbitkit-retry-count")
        if retry_count is not None:
            attrs["messaging.rabbitmq.retry_count"] = str(retry_count)
        return attrs

    def _publish_attributes(self, envelope: MessageEnvelope) -> dict[str, str]:
        attrs: dict[str, str] = {
            "messaging.system": "rabbitmq",
            "messaging.operation": "send",
        }
        if envelope.routing_key:
            attrs["messaging.rabbitmq.destination.routing_key"] = envelope.routing_key
        if envelope.exchange:
            attrs["messaging.destination.name"] = envelope.exchange
        if envelope.message_id:
            attrs["messaging.message.id"] = envelope.message_id
        if envelope.correlation_id:
            attrs["messaging.message.conversation_id"] = envelope.correlation_id
        return attrs

    def _consume_span(self, message: RabbitMessage) -> Any:
        ctx = self._propagate.extract(self._str_carrier(message.headers))
        name = f"{message.headers.get('x-rabbitkit-original-queue', message.routing_key) or 'queue'} receive"
        return self._tracer.start_as_current_span(
            name,
            context=ctx,
            kind=self._trace.SpanKind.CONSUMER,
            attributes=self._consume_attributes(message),
        )

    def _publish_span(self, envelope: MessageEnvelope) -> Any:
        name = f"{envelope.exchange or envelope.routing_key or 'exchange'} send"
        return self._tracer.start_as_current_span(
            name,
            kind=self._trace.SpanKind.PRODUCER,
            attributes=self._publish_attributes(envelope),
        )

    def _record_failure(self, span: Any, exc: BaseException) -> None:
        span.record_exception(exc)
        span.set_status(self._trace.Status(self._trace.StatusCode.ERROR, str(exc)))

    def _envelope_with_context(self, envelope: MessageEnvelope) -> MessageEnvelope:
        """Copy of *envelope* with the CURRENT trace context injected."""
        carrier: dict[str, str] = {}
        self._propagate.inject(carrier)
        if not carrier:
            return envelope
        return replace(envelope, headers={**envelope.headers, **carrier})

    # ── Consume-side hooks ────────────────────────────────────────────────

    def consume_scope(
        self,
        call_next: Callable[[RabbitMessage], Any],
        message: RabbitMessage,
    ) -> Any:
        if self._tracer is None:
            return call_next(message)
        with self._consume_span(message) as span:
            try:
                return call_next(message)
            except BaseException as exc:
                self._record_failure(span, exc)
                raise

    async def consume_scope_async(
        self,
        call_next: Callable[[RabbitMessage], Awaitable[Any]],
        message: RabbitMessage,
    ) -> Any:
        if self._tracer is None:
            return await call_next(message)
        with self._consume_span(message) as span:
            try:
                return await call_next(message)
            except BaseException as exc:
                self._record_failure(span, exc)
                raise

    # ── Publish-side hooks ────────────────────────────────────────────────

    def publish_scope(
        self,
        call_next: Callable[[MessageEnvelope], Any],
        envelope: MessageEnvelope,
    ) -> Any:
        if self._tracer is None:
            return call_next(envelope)
        with self._publish_span(envelope) as span:
            try:
                return call_next(self._envelope_with_context(envelope))
            except BaseException as exc:
                self._record_failure(span, exc)
                raise

    async def publish_scope_async(
        self,
        call_next: Callable[[MessageEnvelope], Awaitable[Any]],
        envelope: MessageEnvelope,
    ) -> Any:
        if self._tracer is None:
            return await call_next(envelope)
        with self._publish_span(envelope) as span:
            try:
                return await call_next(self._envelope_with_context(envelope))
            except BaseException as exc:
                self._record_failure(span, exc)
                raise
