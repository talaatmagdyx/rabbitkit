"""EventMetadataMiddleware — opt-in validation of event metadata.

On consume it reads :class:`~rabbitkit.core.event_metadata.EventMetadata`
from every delivery before the handler runs and applies a supported-versions
policy. A missing or invalid field, or an unsupported ``event_version``,
raises :class:`~rabbitkit.core.errors.EventMetadataError`
(``INVALID_MESSAGE``): never retried, dead-lettered. With a route
``RetryConfig`` (``terminal_action="dead_letter"``, the default) the DLQ
copy carries the failure metadata, because ``RetryMiddleware`` is
auto-inserted outside this middleware.

On publish it validates every envelope that goes through it (the broker's
``middlewares=[...]`` wrap ``broker.publish()``; a route's wrap its handler
result publishes) and raises before anything is sent. It also sets the AMQP
``timestamp`` (publish time) when the envelope has none.

Usage::

    from rabbitkit.middleware.event_metadata import EventMetadataMiddleware

    validate = EventMetadataMiddleware(supported_versions={"order.created": {1, 2}})
    broker = AsyncBroker(config, middlewares=[validate])   # validates broker.publish()

    @broker.subscriber("orders", middlewares=[EventMetadataMiddleware(
        supported_versions={"order.created": {1, 2}}, validate_publish=False)])
    async def handle(body: dict, message: RabbitMessage) -> None:
        meta = EventMetadata.from_message(message)  # already validated
"""

from __future__ import annotations

import dataclasses
from collections.abc import Awaitable, Callable, Mapping
from datetime import UTC, datetime
from typing import Any

from rabbitkit.core.event_metadata import EventMetadata, SupportedVersions, check_supported_version
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.base import BaseMiddleware

__all__ = ["EventMetadataMiddleware"]


def _normalize(supported: SupportedVersions) -> SupportedVersions:
    """Freeze the policy once, at construction (no per-message rebuild)."""
    if supported is None:
        return None
    if isinstance(supported, Mapping):
        return {str(k): frozenset(v) for k, v in supported.items()}
    return frozenset(supported)


class EventMetadataMiddleware(BaseMiddleware):
    """Validate event metadata on consume (before the handler) and on publish.

    Args:
        supported_versions: ``None`` (any ``event_version >= 1``), a
            collection of versions accepted for every event type, or a
            mapping ``event_type -> versions`` (an event type not in the
            mapping is rejected).
        require_tenant: also require the ``tenant-id`` header.
        validate_publish: validate envelopes in ``publish_scope`` (default
            True). Set False on a route whose handler results / RPC replies
            are not events.
    """

    def __init__(
        self,
        *,
        supported_versions: SupportedVersions = None,
        require_tenant: bool = False,
        validate_publish: bool = True,
    ) -> None:
        self._supported = _normalize(supported_versions)
        self._require_tenant = require_tenant
        self._validate_publish = validate_publish

    def validate_message(self, message: RabbitMessage) -> EventMetadata:
        """Read, validate and version-check *message*'s metadata (raises ``EventMetadataError``)."""
        meta = EventMetadata.from_message(message, require_tenant=self._require_tenant)
        check_supported_version(meta, self._supported)
        return meta

    def validate_envelope(self, envelope: MessageEnvelope) -> MessageEnvelope:
        """Validate *envelope*'s metadata; return it with ``timestamp`` set when missing."""
        meta = EventMetadata.from_envelope(envelope, require_tenant=self._require_tenant)
        check_supported_version(meta, self._supported)
        if envelope.timestamp is None:
            return dataclasses.replace(envelope, timestamp=datetime.now(UTC))
        return envelope

    # ── consume ──────────────────────────────────────────────────────────

    def consume_scope(self, call_next: Callable[[RabbitMessage], Any], message: RabbitMessage) -> Any:
        self.validate_message(message)
        return call_next(message)

    async def consume_scope_async(
        self, call_next: Callable[[RabbitMessage], Awaitable[Any]], message: RabbitMessage
    ) -> Any:
        self.validate_message(message)
        return await call_next(message)

    # ── publish ──────────────────────────────────────────────────────────

    def publish_scope(self, call_next: Callable[[MessageEnvelope], Any], envelope: MessageEnvelope) -> Any:
        if self._validate_publish:
            envelope = self.validate_envelope(envelope)
        return call_next(envelope)

    async def publish_scope_async(
        self, call_next: Callable[[MessageEnvelope], Awaitable[Any]], envelope: MessageEnvelope
    ) -> Any:
        if self._validate_publish:
            envelope = self.validate_envelope(envelope)
        return await call_next(envelope)
