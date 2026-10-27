"""Tests for core/event_metadata.py — typed event metadata (G-20 / ADR-M5)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from rabbitkit.core.errors import EventMetadataError, InvalidMessageError, classify_error
from rabbitkit.core.event_metadata import (
    EVENT_METADATA_HEADERS,
    HEADER_EVENT_VERSION,
    HEADER_IDEMPOTENCY_KEY,
    HEADER_OCCURRED_AT,
    HEADER_TENANT_ID,
    MAX_EVENT_FIELD_BYTES,
    EventMetadata,
    check_supported_version,
    format_occurred_at,
)
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import ErrorCategory, MessageEnvelope

OCCURRED = datetime(2026, 10, 27, 12, 30, 15, 123456, tzinfo=UTC)


def _meta(**kw: Any) -> EventMetadata:
    fields: dict[str, Any] = {
        "event_type": "order.created",
        "event_version": 2,
        "source": "checkout",
        "idempotency_key": "order:42",
        "occurred_at": OCCURRED,
        "message_id": "m-1",
        "correlation_id": "c-1",
    }
    fields.update(kw)
    return EventMetadata(**fields)


def _message_from(envelope: MessageEnvelope) -> RabbitMessage:
    return RabbitMessage(
        body=envelope.body,
        headers=dict(envelope.headers),
        message_id=envelope.message_id,
        correlation_id=envelope.correlation_id,
        type=envelope.type,
        app_id=envelope.app_id,
        timestamp=envelope.timestamp,
    )


class TestConstruction:
    def test_valid_fields_and_utc_normalisation(self) -> None:
        plus2 = timezone(timedelta(hours=2))
        meta = _meta(occurred_at=datetime(2026, 10, 27, 14, 0, tzinfo=plus2), tenant_id="t-1")
        assert meta.occurred_at == datetime(2026, 10, 27, 12, 0, tzinfo=UTC)
        assert meta.occurred_at.tzinfo is UTC
        assert meta.tenant_id == "t-1"

    def test_frozen(self) -> None:
        meta = _meta()
        with pytest.raises(AttributeError):
            meta.event_version = 3  # type: ignore[misc]

    @pytest.mark.parametrize(
        ("change", "field"),
        [
            ({"message_id": ""}, "message_id"),
            ({"idempotency_key": "   "}, "idempotency_key"),
            ({"event_type": None}, "event_type"),
            ({"event_version": 0}, "event_version"),
            ({"event_version": True}, "event_version"),
            ({"event_version": "1"}, "event_version"),
            ({"source": "x" * (MAX_EVENT_FIELD_BYTES + 1)}, "source"),
            ({"occurred_at": datetime(2026, 1, 1)}, "occurred_at"),  # naive
            ({"occurred_at": "2026-01-01T00:00:00Z"}, "occurred_at"),
            ({"correlation_id": 7}, "correlation_id"),
            ({"tenant_id": ""}, "tenant_id"),
        ],
    )
    def test_invalid_field_is_named(self, change: dict[str, Any], field: str) -> None:
        with pytest.raises(EventMetadataError) as info:
            _meta(**change)
        assert info.value.fields == (field,)
        assert field in str(info.value)

    def test_error_is_an_invalid_message_and_hides_values(self) -> None:
        with pytest.raises(EventMetadataError) as info:
            _meta(idempotency_key="", source="secret-source-" + "x" * 300)
        exc = info.value
        assert isinstance(exc, InvalidMessageError) and isinstance(exc, ValueError)
        assert classify_error(exc).category is ErrorCategory.INVALID_MESSAGE
        assert exc.fields == ("idempotency_key", "source")
        assert "secret-source" not in str(exc)

    def test_multibyte_length_is_measured_in_bytes(self) -> None:
        _meta(source="é" * (MAX_EVENT_FIELD_BYTES // 2))
        with pytest.raises(EventMetadataError):
            _meta(source="é" * (MAX_EVENT_FIELD_BYTES // 2 + 1))

    def test_create_fills_in_ids_and_time(self) -> None:
        before = datetime.now(UTC)
        meta = EventMetadata.create(event_type="a.b", event_version=1, source="svc", idempotency_key="k")
        assert meta.message_id and meta.correlation_id == meta.message_id
        assert before <= meta.occurred_at <= datetime.now(UTC)
        assert meta.tenant_id is None

    def test_create_keeps_given_values(self) -> None:
        meta = EventMetadata.create(
            event_type="a.b",
            event_version=1,
            source="svc",
            idempotency_key="k",
            occurred_at=OCCURRED,
            message_id="mid",
            correlation_id="cid",
            tenant_id="t",
        )
        assert (meta.message_id, meta.correlation_id, meta.occurred_at, meta.tenant_id) == ("mid", "cid", OCCURRED, "t")


class TestFormat:
    def test_rfc3339_utc_with_z(self) -> None:
        assert format_occurred_at(OCCURRED) == "2026-10-27T12:30:15.123456Z"
        assert format_occurred_at(OCCURRED.replace(microsecond=0)) == "2026-10-27T12:30:15Z"

    def test_naive_is_rejected(self) -> None:
        with pytest.raises(EventMetadataError):
            format_occurred_at(datetime(2026, 1, 1))


class TestEnvelope:
    def test_to_envelope_maps_properties_and_headers(self) -> None:
        meta = _meta(tenant_id="t-1")
        env = meta.to_envelope(routing_key="orders", body=b"{}", headers={"x-app": "1"}, priority=3)
        assert env.message_id == "m-1"
        assert env.correlation_id == "c-1"
        assert env.type == "order.created"
        assert env.app_id == "checkout"
        assert env.priority == 3
        assert env.timestamp is not None and env.timestamp.tzinfo is UTC
        assert env.headers == {
            "x-app": "1",
            HEADER_IDEMPOTENCY_KEY: "order:42",
            HEADER_EVENT_VERSION: 2,
            HEADER_OCCURRED_AT: "2026-10-27T12:30:15.123456Z",
            HEADER_TENANT_ID: "t-1",
        }
        assert set(meta.headers()) == EVENT_METADATA_HEADERS

    def test_tenant_header_only_when_set(self) -> None:
        assert HEADER_TENANT_ID not in _meta().headers()

    def test_to_envelope_rejects_message_id(self) -> None:
        with pytest.raises(TypeError, match="message_id"):
            _meta().to_envelope(routing_key="q", body=b"", message_id="other")

    def test_apply_to_keeps_an_existing_timestamp(self) -> None:
        stamp = datetime(2026, 1, 1, tzinfo=UTC)
        env = _meta().apply_to(MessageEnvelope(routing_key="q", body=b"", timestamp=stamp))
        assert env.timestamp == stamp

    def test_apply_to_accepts_matching_values(self) -> None:
        meta = _meta()
        env = MessageEnvelope(
            routing_key="q",
            body=b"",
            type="order.created",
            app_id="checkout",
            correlation_id="c-1",
            headers=meta.headers(),
        )
        assert meta.apply_to(env).headers == meta.headers()

    @pytest.mark.parametrize(
        ("fields", "conflict"),
        [
            ({"type": "other"}, "event_type"),
            ({"app_id": "other"}, "source"),
            ({"correlation_id": "other"}, "correlation_id"),
            ({"headers": {HEADER_EVENT_VERSION: 9}}, HEADER_EVENT_VERSION),
            ({"headers": {HEADER_TENANT_ID: "t"}}, HEADER_TENANT_ID),
        ],
    )
    def test_apply_to_refuses_to_overwrite(self, fields: dict[str, Any], conflict: str) -> None:
        with pytest.raises(EventMetadataError) as info:
            _meta().apply_to(MessageEnvelope(routing_key="q", body=b"", **fields))
        assert info.value.fields == (conflict,)


class TestRead:
    def test_round_trip_through_a_message(self) -> None:
        meta = _meta(tenant_id="t-1")
        env = meta.to_envelope(routing_key="q", body=b"{}")
        assert EventMetadata.from_message(_message_from(env)) == meta
        assert EventMetadata.from_envelope(env) == meta

    def test_bytes_and_string_header_values_are_accepted(self) -> None:
        msg = _message_from(_meta().to_envelope(routing_key="q", body=b""))
        msg.headers[HEADER_IDEMPOTENCY_KEY] = b"order:42"
        msg.headers[HEADER_EVENT_VERSION] = b"2"
        msg.headers[HEADER_OCCURRED_AT] = "2026-10-27T14:30:15.123456+02:00"
        assert EventMetadata.from_message(msg) == _meta()

    def test_amqp_timestamp_header_is_utc(self) -> None:
        msg = _message_from(_meta().to_envelope(routing_key="q", body=b""))
        msg.headers[HEADER_OCCURRED_AT] = OCCURRED.replace(tzinfo=None)  # client libraries decode naive
        assert EventMetadata.from_message(msg).occurred_at == OCCURRED
        msg.headers[HEADER_OCCURRED_AT] = OCCURRED
        assert EventMetadata.from_message(msg).occurred_at == OCCURRED

    def test_everything_missing_is_listed(self) -> None:
        with pytest.raises(EventMetadataError) as info:
            EventMetadata.from_message(RabbitMessage(body=b""))
        assert info.value.fields == (
            "message_id",
            "idempotency_key",
            "event_type",
            "event_version",
            "source",
            "occurred_at",
            "correlation_id",
        )
        assert "missing or invalid" in str(info.value)

    @pytest.mark.parametrize(
        ("header", "value", "field"),
        [
            (HEADER_EVENT_VERSION, "v2", "event_version"),
            (HEADER_EVENT_VERSION, True, "event_version"),
            (HEADER_EVENT_VERSION, 1.5, "event_version"),
            (HEADER_EVENT_VERSION, "²", "event_version"),
            (HEADER_OCCURRED_AT, "yesterday", "occurred_at"),
            (HEADER_OCCURRED_AT, "2026-10-27T12:00:00", "occurred_at"),  # no offset
            (HEADER_IDEMPOTENCY_KEY, b"\xff\xfe", "idempotency_key"),
            (HEADER_IDEMPOTENCY_KEY, 42, "idempotency_key"),
            (HEADER_TENANT_ID, "", "tenant_id"),
        ],
    )
    def test_invalid_header_is_named(self, header: str, value: Any, field: str) -> None:
        msg = _message_from(_meta().to_envelope(routing_key="q", body=b""))
        msg.headers[header] = value
        with pytest.raises(EventMetadataError) as info:
            EventMetadata.from_message(msg)
        assert info.value.fields == (field,)

    def test_version_zero_is_invalid(self) -> None:
        msg = _message_from(_meta().to_envelope(routing_key="q", body=b""))
        msg.headers[HEADER_EVENT_VERSION] = 0
        with pytest.raises(EventMetadataError) as info:
            EventMetadata.from_message(msg)
        assert info.value.fields == ("event_version",)

    def test_require_tenant(self) -> None:
        msg = _message_from(_meta().to_envelope(routing_key="q", body=b""))
        assert EventMetadata.from_message(msg).tenant_id is None
        with pytest.raises(EventMetadataError) as info:
            EventMetadata.from_message(msg, require_tenant=True)
        assert info.value.fields == ("tenant_id",)


class TestSupportedVersions:
    def test_none_accepts_everything(self) -> None:
        check_supported_version(_meta(event_version=99), None)

    def test_collection_applies_to_every_type(self) -> None:
        check_supported_version(_meta(event_version=2), {1, 2})
        with pytest.raises(EventMetadataError) as info:
            check_supported_version(_meta(event_version=3), (1, 2))
        assert info.value.fields == ("event_version",)

    def test_mapping_per_event_type(self) -> None:
        policy = {"order.created": range(1, 3)}
        check_supported_version(_meta(event_version=2), policy)
        with pytest.raises(EventMetadataError) as info:
            check_supported_version(_meta(event_version=3), policy)
        assert info.value.fields == ("event_version",)
        with pytest.raises(EventMetadataError) as info:
            check_supported_version(_meta(event_type="order.cancelled"), policy)
        assert info.value.fields == ("event_type",)
