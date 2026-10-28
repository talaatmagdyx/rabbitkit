"""Optional, generic event metadata carried in AMQP properties and headers.

An *event* published through RabbitMQ usually needs more identity than a
routing key and a body: what kind of event it is, which schema version the
body follows, who produced it, when it happened, and a business key a
consumer can deduplicate on. :class:`EventMetadata` maps those fields onto
AMQP properties and a few headers, so the body stays pure payload (any
codec) and the broker, the DLQ tooling and tracing can read identity
without decoding it:

=================  ==================================  =========================
Field              AMQP location                       Required on read
=================  ==================================  =========================
``message_id``     ``message_id`` property             yes
``idempotency_key``  header ``idempotency-key``        yes
``event_type``     ``type`` property                   yes
``event_version``  header ``event-version`` (int)      yes
``source``         ``app_id`` property                 yes
``occurred_at``    header ``occurred-at`` (RFC 3339)   yes
``correlation_id`` ``correlation_id`` property         yes
``tenant_id``      header ``tenant-id``                no (opt-in requirement)
=================  ==================================  =========================

Trace context (``traceparent`` / ``tracestate``) is not part of it: the
OpenTelemetry middleware owns those headers. The AMQP ``timestamp``
property is the *publish* time; ``occurred_at`` is the business event time.

The feature is opt-in. Nothing reads or requires these fields unless the
application builds envelopes with :meth:`EventMetadata.to_envelope` /
:meth:`EventMetadata.apply_to`, reads them with
:meth:`EventMetadata.from_message`, or adds
:class:`~rabbitkit.middleware.event_metadata.EventMetadataMiddleware`.

Retry delay-queue copies, terminal DLQ copies and DLQ replays copy every
AMQP property and header, so the metadata survives all of them unchanged.
"""

from __future__ import annotations

import dataclasses
import uuid
from collections.abc import Collection, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, TypeAlias

from rabbitkit.core.errors import EventMetadataError
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope

__all__ = [
    "EVENT_METADATA_HEADERS",
    "HEADER_EVENT_VERSION",
    "HEADER_IDEMPOTENCY_KEY",
    "HEADER_OCCURRED_AT",
    "HEADER_TENANT_ID",
    "MAX_EVENT_FIELD_BYTES",
    "EventMetadata",
    "SupportedVersions",
    "check_supported_version",
    "format_occurred_at",
]

HEADER_IDEMPOTENCY_KEY = "idempotency-key"
HEADER_EVENT_VERSION = "event-version"
HEADER_OCCURRED_AT = "occurred-at"
HEADER_TENANT_ID = "tenant-id"

#: The headers :class:`EventMetadata` owns.
EVENT_METADATA_HEADERS = frozenset({HEADER_IDEMPOTENCY_KEY, HEADER_EVENT_VERSION, HEADER_OCCURRED_AT, HEADER_TENANT_ID})

#: Upper bound (UTF-8 bytes) for every string field. ``message_id``,
#: ``type``, ``app_id`` and ``correlation_id`` are AMQP short strings, which
#: cannot be longer; the header fields use the same bound. Hash a longer
#: business key before using it as ``idempotency_key``.
MAX_EVENT_FIELD_BYTES = 255

#: A supported-versions policy: ``None`` (any version), a collection of
#: versions accepted for every event type, or a mapping of event type to
#: its accepted versions (an event type missing from the mapping is
#: rejected).
SupportedVersions: TypeAlias = Mapping[str, Collection[int]] | Collection[int] | None


def format_occurred_at(value: datetime) -> str:
    """RFC 3339 UTC text for *value* (``2026-10-27T12:00:00.123456Z``).

    Microseconds are kept (omitted when zero), so the value reads back
    exactly. *value* must be timezone-aware.
    """
    if value.tzinfo is None or value.utcoffset() is None:
        raise EventMetadataError("occurred_at must be timezone-aware", fields=("occurred_at",))
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


def _text_field(name: str, value: Any, problems: list[str]) -> None:
    if not isinstance(value, str) or not value.strip():
        problems.append(name)
        return
    if len(value.encode("utf-8")) > MAX_EVENT_FIELD_BYTES:
        problems.append(name)


def _invalid(problems: list[str], *, missing: bool = False) -> EventMetadataError:
    what = "missing or invalid" if missing else "invalid"
    return EventMetadataError(
        f"Event metadata {what}: {', '.join(problems)} (strings must be non-empty and at most "
        f"{MAX_EVENT_FIELD_BYTES} UTF-8 bytes; event_version an int >= 1; occurred_at "
        "timezone-aware / RFC 3339 with an offset)",
        fields=tuple(problems),
    )


@dataclass(frozen=True, slots=True, kw_only=True)
class EventMetadata:
    """Typed, validated event identity (see the module docstring for the AMQP mapping).

    Construction validates every field and raises
    :class:`~rabbitkit.core.errors.EventMetadataError` (an
    ``InvalidMessageError``, so a ``ValueError``) naming the bad fields;
    ``occurred_at`` is normalised to UTC. Use :meth:`create` to fill in
    ``message_id``, ``correlation_id`` and ``occurred_at``.

    Example::

        meta = EventMetadata.create(
            event_type="order.created",
            event_version=1,
            source="checkout-service",
            idempotency_key="order:1234",
        )
        await broker.publish(meta.to_envelope(routing_key="orders", body=b"{...}"))
    """

    event_type: str
    event_version: int
    source: str
    idempotency_key: str
    occurred_at: datetime
    message_id: str
    correlation_id: str
    tenant_id: str | None = None

    def __post_init__(self) -> None:
        problems: list[str] = []
        _text_field("message_id", self.message_id, problems)
        _text_field("idempotency_key", self.idempotency_key, problems)
        _text_field("event_type", self.event_type, problems)
        if isinstance(self.event_version, bool) or not isinstance(self.event_version, int) or self.event_version < 1:
            problems.append("event_version")
        _text_field("source", self.source, problems)
        if (
            not isinstance(self.occurred_at, datetime)
            or self.occurred_at.tzinfo is None
            or self.occurred_at.utcoffset() is None
        ):
            problems.append("occurred_at")
        _text_field("correlation_id", self.correlation_id, problems)
        if self.tenant_id is not None:
            _text_field("tenant_id", self.tenant_id, problems)
        if problems:
            raise _invalid(problems)
        object.__setattr__(self, "occurred_at", self.occurred_at.astimezone(UTC))

    # ── construction ─────────────────────────────────────────────────────

    @classmethod
    def create(
        cls,
        *,
        event_type: str,
        event_version: int,
        source: str,
        idempotency_key: str,
        occurred_at: datetime | None = None,
        message_id: str | None = None,
        correlation_id: str | None = None,
        tenant_id: str | None = None,
    ) -> EventMetadata:
        """Build metadata for a new event.

        ``message_id`` defaults to a fresh UUID (pass your own, ideally
        time-ordered, to republish the same logical message after an unknown
        publish outcome); ``correlation_id`` defaults to ``message_id`` (the
        start of a new chain); ``occurred_at`` defaults to now (UTC).
        """
        mid = message_id if message_id is not None else str(uuid.uuid4())
        return cls(
            event_type=event_type,
            event_version=event_version,
            source=source,
            idempotency_key=idempotency_key,
            occurred_at=occurred_at if occurred_at is not None else datetime.now(UTC),
            message_id=mid,
            correlation_id=correlation_id if correlation_id is not None else mid,
            tenant_id=tenant_id,
        )

    # ── reading ──────────────────────────────────────────────────────────

    @classmethod
    def from_message(cls, message: RabbitMessage, *, require_tenant: bool = False) -> EventMetadata:
        """Read and validate the metadata of a delivery.

        Raises :class:`~rabbitkit.core.errors.EventMetadataError`
        (``INVALID_MESSAGE``: dead-lettered, never retried) when a required
        field is missing or invalid, or when *require_tenant* is set and
        ``tenant-id`` is absent.
        """
        return cls._read(
            message.headers,
            message_id=message.message_id,
            event_type=message.type,
            source=message.app_id,
            correlation_id=message.correlation_id,
            require_tenant=require_tenant,
        )

    @classmethod
    def from_envelope(cls, envelope: MessageEnvelope, *, require_tenant: bool = False) -> EventMetadata:
        """Read and validate the metadata of an outgoing envelope (see :meth:`from_message`)."""
        return cls._read(
            envelope.headers,
            message_id=envelope.message_id,
            event_type=envelope.type,
            source=envelope.app_id,
            correlation_id=envelope.correlation_id,
            require_tenant=require_tenant,
        )

    @classmethod
    def _read(
        cls,
        headers: Mapping[str, Any],
        *,
        message_id: Any,
        event_type: Any,
        source: Any,
        correlation_id: Any,
        require_tenant: bool,
    ) -> EventMetadata:
        problems: list[str] = []
        idempotency_key = _header_text(headers.get(HEADER_IDEMPOTENCY_KEY))
        version = _header_version(headers.get(HEADER_EVENT_VERSION))
        occurred_at = _header_datetime(headers.get(HEADER_OCCURRED_AT))
        tenant_raw = headers.get(HEADER_TENANT_ID)
        tenant_id = _header_text(tenant_raw)
        for name, value in (
            ("message_id", message_id),
            ("idempotency_key", idempotency_key),
            ("event_type", event_type),
            ("event_version", version),
            ("source", source),
            ("occurred_at", occurred_at),
            ("correlation_id", correlation_id),
        ):
            if value is None or value == "":
                problems.append(name)
        if (tenant_raw is not None and tenant_id is None) or (require_tenant and tenant_id is None):
            problems.append("tenant_id")
        if problems:
            raise _invalid(problems, missing=True)
        # Narrowed by the loop above.
        assert idempotency_key is not None and version is not None and occurred_at is not None
        return cls(
            event_type=event_type,
            event_version=version,
            source=source,
            idempotency_key=idempotency_key,
            occurred_at=occurred_at,
            message_id=message_id,
            correlation_id=correlation_id,
            tenant_id=tenant_id,
        )

    # ── writing ──────────────────────────────────────────────────────────

    def headers(self) -> dict[str, Any]:
        """The metadata headers (``idempotency-key``, ``event-version``,
        ``occurred-at`` and, when set, ``tenant-id``)."""
        out: dict[str, Any] = {
            HEADER_IDEMPOTENCY_KEY: self.idempotency_key,
            HEADER_EVENT_VERSION: self.event_version,
            HEADER_OCCURRED_AT: format_occurred_at(self.occurred_at),
        }
        if self.tenant_id is not None:
            out[HEADER_TENANT_ID] = self.tenant_id
        return out

    def apply_to(self, envelope: MessageEnvelope) -> MessageEnvelope:
        """A copy of *envelope* carrying this metadata.

        Sets ``message_id`` (replacing the envelope's, which is a random
        default unless set on purpose), ``type``, ``app_id``,
        ``correlation_id`` and the metadata headers, and the AMQP
        ``timestamp`` (publish time) to now when the envelope has none.
        An envelope that already carries a *different* ``type``, ``app_id``,
        ``correlation_id`` or metadata header raises
        :class:`~rabbitkit.core.errors.EventMetadataError` instead of being
        silently overwritten.
        """
        conflicts = [
            name
            for name, current, wanted in (
                ("event_type", envelope.type, self.event_type),
                ("source", envelope.app_id, self.source),
                ("correlation_id", envelope.correlation_id, self.correlation_id),
            )
            if current is not None and current != wanted
        ]
        meta_headers = self.headers()
        conflicts.extend(
            name for name, value in meta_headers.items() if name in envelope.headers and envelope.headers[name] != value
        )
        if HEADER_TENANT_ID in envelope.headers and HEADER_TENANT_ID not in meta_headers:
            conflicts.append(HEADER_TENANT_ID)
        if conflicts:
            raise EventMetadataError(
                f"Envelope already carries different values for: {', '.join(conflicts)}",
                fields=tuple(conflicts),
            )
        return dataclasses.replace(
            envelope,
            headers={**envelope.headers, **meta_headers},
            message_id=self.message_id,
            correlation_id=self.correlation_id,
            type=self.event_type,
            app_id=self.source,
            timestamp=envelope.timestamp if envelope.timestamp is not None else datetime.now(UTC),
        )

    def to_envelope(
        self,
        *,
        routing_key: str,
        body: bytes,
        exchange: str = "",
        headers: Mapping[str, Any] | None = None,
        **envelope_fields: Any,
    ) -> MessageEnvelope:
        """Build a :class:`~rabbitkit.core.types.MessageEnvelope` carrying this metadata.

        *envelope_fields* are any other ``MessageEnvelope`` fields
        (``content_type``, ``mandatory``, ``delivery_mode``, ``expiration``
        ...). ``message_id`` is not accepted there: it comes from the
        metadata. See :meth:`apply_to` for what is set.
        """
        if "message_id" in envelope_fields:
            raise TypeError("message_id comes from EventMetadata.message_id; do not pass it to to_envelope()")
        envelope = MessageEnvelope(
            routing_key=routing_key,
            body=body,
            exchange=exchange,
            headers=dict(headers) if headers is not None else {},
            **envelope_fields,
        )
        return self.apply_to(envelope)


def check_supported_version(metadata: EventMetadata, supported: SupportedVersions) -> None:
    """Raise :class:`~rabbitkit.core.errors.EventMetadataError` when
    *metadata*'s version (or, with a mapping, its event type) is not in *supported*.

    ``None`` accepts every version.
    """
    if supported is None:
        return
    if isinstance(supported, Mapping):
        versions = supported.get(metadata.event_type)
        if versions is None:
            raise EventMetadataError(
                "Unsupported event_type (no supported versions configured)", fields=("event_type",)
            )
    else:
        versions = supported
    if metadata.event_version not in versions:
        raise EventMetadataError(
            f"Unsupported event_version {metadata.event_version} for this event_type", fields=("event_version",)
        )


# ── header decoding ─────────────────────────────────────────────────────────


def _header_text(value: Any) -> str | None:
    if isinstance(value, bytes):
        try:
            value = value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(value, str) and value:
        return value
    return None


def _header_version(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    text = _header_text(value)
    if text is not None and text.isascii() and text.isdigit():
        return int(text)
    return None


def _header_datetime(value: Any) -> datetime | None:
    if isinstance(value, datetime):
        # An AMQP timestamp field is UTC by definition; client libraries
        # decode it naive.
        return value if value.tzinfo is not None else value.replace(tzinfo=UTC)
    text = _header_text(value)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return parsed if parsed.tzinfo is not None else None
