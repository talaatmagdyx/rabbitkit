"""Pyright-strict probe of rabbitkit's PUBLIC API (gap G-29).

This module is never executed: CI runs ``pyright -p tests/typing`` over it in
strict mode and requires 0 errors. It uses the public API the way a strictly
typed downstream application would, and pins the types it relies on with
``assert_type`` so a public signature that silently degrades to ``Any`` (or
loses a generic) fails the check instead of passing unnoticed.

Rules for editing:

* import only from ``rabbitkit`` (the top-level package) or a documented
  public submodule (``rabbitkit.testing``) -- never a private module;
* no ``# type: ignore`` / ``# pyright: ignore``: if the public API needs a
  suppression to be used, that is the defect this file exists to catch;
* nothing runs at import time (all usage lives in functions).

Not collected by pytest (the file name does not match ``test_*.py``) and not
part of the mypy gate (which covers ``src/rabbitkit`` only).
"""

from __future__ import annotations

from datetime import UTC, datetime
from types import CoroutineType
from typing import Any, assert_type

from rabbitkit import (
    AsyncBroker,
    BrokerHealthResult,
    BulkPublishItem,
    BulkPublishResult,
    BulkPublishStatus,
    ClassifiedError,
    ConnectionConfig,
    ConsumerConfig,
    ErrorCategory,
    ErrorSeverity,
    EventMetadata,
    EventMetadataError,
    HealthCheckConfig,
    HealthStatus,
    InMemoryOffsetStore,
    MessageEnvelope,
    OffsetStore,
    PublisherConfig,
    PublishOutcome,
    PublishReason,
    PublishStatus,
    QueueType,
    RabbitConfig,
    RabbitMessage,
    RabbitQueue,
    ReliabilityProfile,
    RetryConfig,
    SQLiteOffsetStore,
    StreamConsumerConfig,
    StreamOffset,
    StreamOffsetType,
    SyncBroker,
    apply_profile,
    broker_health_check,
    broker_health_check_async,
    broker_liveness,
    broker_readiness_async,
    critical_config,
)
from rabbitkit.testing import MockedHandler, TestBroker

# ── configuration ─────────────────────────────────────────────────────────────


def build_config(url: str) -> RabbitConfig:
    config = RabbitConfig(
        connection=ConnectionConfig.from_url(url),
        consumer=ConsumerConfig(prefetch_count=50, processing_timeout=30.0),
        publisher=PublisherConfig(confirm_delivery=True, mandatory=True, confirm_timeout=5.0),
    )
    assert_type(config.connection, ConnectionConfig)
    assert_type(config.publisher.confirm_timeout, float)
    assert_type(config.consumer.processing_timeout, float | None)
    return config


def build_critical_config(url: str) -> RabbitConfig:
    base = build_config(url)
    critical = critical_config(base)
    same = apply_profile(base, ReliabilityProfile.CRITICAL)
    by_name = apply_profile(base, "critical")
    assert_type(critical, RabbitConfig)
    assert_type(same, RabbitConfig)
    assert_type(by_name, RabbitConfig)
    return critical


def orders_queue() -> RabbitQueue:
    return RabbitQueue(name="orders", durable=True, queue_type=QueueType.QUORUM)


def retry_policy() -> RetryConfig:
    return RetryConfig(max_retries=3, delays=(1, 5, 30))


# ── event metadata ────────────────────────────────────────────────────────────


def build_event(order_id: str) -> MessageEnvelope:
    metadata = EventMetadata.create(
        event_type="order.created",
        event_version=1,
        source="orders-service",
        idempotency_key=order_id,
        occurred_at=datetime.now(UTC),
        tenant_id="tenant-1",
    )
    assert_type(metadata.message_id, str)
    assert_type(metadata.occurred_at, datetime)
    assert_type(metadata.tenant_id, str | None)
    envelope = metadata.apply_to(MessageEnvelope(routing_key="orders", body=b"{}", mandatory=True))
    assert_type(envelope, MessageEnvelope)
    return envelope


def read_event(message: RabbitMessage) -> EventMetadata | None:
    try:
        metadata = EventMetadata.from_message(message, require_tenant=True)
    except EventMetadataError:
        return None
    assert_type(metadata.event_type, str)
    assert_type(metadata.event_version, int)
    return metadata


# ── publish outcomes ──────────────────────────────────────────────────────────


def handle_outcome(outcome: PublishOutcome) -> str:
    assert_type(outcome.status, PublishStatus)
    assert_type(outcome.reason, str | None)
    assert_type(outcome.reply_code, int | None)
    assert_type(outcome.reply_text, str | None)
    assert_type(outcome.is_unknown, bool)
    assert_type(outcome.ok, bool)
    if outcome.status is PublishStatus.CONFIRMED:
        return "durable"
    if outcome.is_unknown:
        return "republish with the same message_id"
    if outcome.reason == PublishReason.NACKED:
        return "broker refused"
    return outcome.status.value


def handle_bulk(result: BulkPublishResult) -> list[int]:
    assert_type(result.items, tuple[BulkPublishItem, ...])
    assert_type(result.counts, dict[BulkPublishStatus, int])
    retry_later: list[int] = []
    for item in result.items:
        assert_type(item.status, BulkPublishStatus)
        assert_type(item.reason, str)
        if item.status in (BulkPublishStatus.NOT_SENT, BulkPublishStatus.UNKNOWN):
            retry_later.append(item.index)
    return retry_later


def categorise(error: ClassifiedError) -> bool:
    assert_type(error.category, ErrorCategory)
    assert_type(error.severity, ErrorSeverity)
    return error.category in (ErrorCategory.RETRYABLE, ErrorCategory.INFRASTRUCTURE)


# ── async application ─────────────────────────────────────────────────────────


def build_async_app(url: str, received: list[bytes], probe: RabbitMessage) -> AsyncBroker:
    broker = AsyncBroker(build_critical_config(url))

    @broker.subscriber(queue=orders_queue(), retry=retry_policy())
    async def on_order(message: RabbitMessage) -> None:
        assert_type(message.body, bytes)
        if read_event(message) is not None:
            received.append(message.body)

    # The decorator returns the handler unchanged, signature included: an
    # erased ``(...) -> Any`` would make this ``Any`` and fail the check.
    pending = assert_type(on_order(probe), CoroutineType[Any, Any, None])
    pending.close()
    return broker


async def run_async_app(url: str, probe: RabbitMessage) -> list[str]:
    received: list[bytes] = []
    async with build_async_app(url, received, probe) as broker:
        assert_type(broker, AsyncBroker)
        outcome = await broker.publish(build_event("o-1"))
        assert_type(outcome, PublishOutcome)
        bulk = await broker.publish_many([build_event("o-2"), build_event("o-3")])
        assert_type(bulk, BulkPublishResult)
        health = await broker_health_check_async(broker, HealthCheckConfig())
        assert_type(health, BrokerHealthResult)
        assert_type(health.status, HealthStatus)
        ready = await broker_readiness_async(broker)
        assert_type(ready, bool)
        return [handle_outcome(outcome), *map(str, handle_bulk(bulk))]


# ── sync application ──────────────────────────────────────────────────────────


def run_sync_app(url: str, probe: RabbitMessage) -> str:
    broker = SyncBroker(build_config(url))

    @broker.subscriber(queue="orders-sync", retry=retry_policy())
    def on_order(message: RabbitMessage) -> None:
        message.ack()

    assert_type(on_order(probe), None)
    broker.start()
    try:
        outcome = broker.publish(build_event("o-4"))
        assert_type(outcome, PublishOutcome)
        bulk = broker.publish_many([build_event("o-5")])
        assert_type(bulk, BulkPublishResult)
        health = broker_health_check(broker)
        assert_type(health, BrokerHealthResult)
        alive = broker_liveness(broker)
        assert_type(alive, bool)
        return handle_outcome(outcome)
    finally:
        broker.stop()


# ── tests with the in-memory TestBroker ───────────────────────────────────────


def exercise_test_broker() -> int:
    broker = TestBroker()
    seen: list[bytes] = []

    @broker.subscriber(queue="orders")
    def on_order(body: bytes) -> None:
        seen.append(body)

    handler: MockedHandler[[bytes], None] = on_order
    assert_type(on_order(b"direct call keeps its signature"), None)
    broker.start()
    try:
        broker.publish("orders", b'{"id": 1}')
        handler.mock.assert_called_once_with(b'{"id": 1}')
        consumed = broker.consumed_messages
        assert_type(consumed, list[RabbitMessage])
        broker.assert_acked(consumed[0])
        assert_type(broker.published_messages, list[MessageEnvelope])
        broker.publish_outcome = PublishOutcome(status=PublishStatus.NACKED, reason=PublishReason.NACKED)
        broker.fail_next_publish()
    finally:
        broker.stop()
    return len(seen)


# ── stream queues ─────────────────────────────────────────────────────────────


def exercise_streams(path: str) -> None:
    broker = AsyncBroker(RabbitConfig())
    events = RabbitQueue("events", queue_type=QueueType.STREAM, max_age="7D", max_length_bytes=10_000_000)
    store: OffsetStore = SQLiteOffsetStore(path)
    config = StreamConsumerConfig(offset=StreamOffset.first(), offset_store=store, commit_every=50)
    assert_type(config.offset, StreamOffset)
    assert_type(config.offset.type, StreamOffsetType)
    assert_type(store.load("events"), int | None)
    memory: OffsetStore = InMemoryOffsetStore()
    memory.save("events", 3)

    @broker.subscriber(events, stream=config)
    async def project(body: bytes, message: RabbitMessage) -> None:
        assert_type(message.stream_offset, int | None)

    @broker.subscriber(RabbitQueue("audit", queue_type=QueueType.STREAM), stream_offset=datetime.now(UTC))
    async def audit(body: bytes) -> None: ...

    sync = SyncBroker(RabbitConfig())

    @sync.subscriber(RabbitQueue("replay", queue_type=QueueType.STREAM), stream_offset=42)
    def replay(body: bytes) -> None: ...

    assert_type(StreamOffset.parse("first"), StreamOffset)
