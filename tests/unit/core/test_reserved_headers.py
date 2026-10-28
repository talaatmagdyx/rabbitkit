"""Tests for core/reserved_headers.py and ``PublisherConfig.strip_reserved_headers``:
an application publish cannot carry rabbitkit's retry bookkeeping;
the library's own republishes keep it."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any
from unittest.mock import AsyncMock, MagicMock, patch

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.core.config import PublisherConfig, RabbitConfig, RetryConfig
from rabbitkit.core.reserved_headers import RESERVED_HEADERS, ReservedHeaderStripper, strip_reserved_headers
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.sync.broker import SyncBroker

if TYPE_CHECKING:
    import pytest

FORGED = {
    "x-rabbitkit-retry-count": 99,
    "x-rabbitkit-first-failed-at": "2000-01-01T00:00:00+00:00",
    "x-rabbitkit-original-queue": "orders",
    "x-death": [{"queue": "orders.retry.4", "count": 1}],
    "x-tenant": "acme",
    "traceparent": "00-abc-def-01",
}
KEPT = {"x-tenant": "acme", "traceparent": "00-abc-def-01"}


class TestStripReservedHeaders:
    def test_nothing_reserved_returns_none(self) -> None:
        assert strip_reserved_headers({"x-tenant": "acme"}) is None
        assert strip_reserved_headers({}) is None

    def test_reserved_headers_are_removed(self) -> None:
        assert strip_reserved_headers(FORGED) == KEPT

    def test_extra_names(self) -> None:
        assert strip_reserved_headers({"x-my-retry": 3, "a": 1}, extra=("x-my-retry",)) == {"a": 1}

    def test_the_retry_metadata_headers_are_all_reserved(self) -> None:
        for name in ("x-rabbitkit-retry-count", "x-rabbitkit-error-category", "x-rabbitkit-replay-count", "x-death"):
            assert name in RESERVED_HEADERS
        assert "x-rabbitkit-signature" not in RESERVED_HEADERS  # added by SigningMiddleware after the strip


class TestStripper:
    def test_untouched_envelope_is_returned_as_is(self) -> None:
        env = MessageEnvelope(routing_key="q", body=b"", headers={"a": 1})
        assert ReservedHeaderStripper()(env) is env

    def test_strips_and_warns_once_per_header(self, caplog: pytest.LogCaptureFixture) -> None:
        stripper = ReservedHeaderStripper(extra=("x-my-retry", "x-rabbitkit-retry-count"))
        env = MessageEnvelope(routing_key="q", body=b"x", headers={**FORGED, "x-my-retry": 1}, message_id="m")
        with caplog.at_level(logging.WARNING, logger="rabbitkit.core.reserved_headers"):
            out = stripper(env)
            stripper(env)
        assert out.headers == KEPT
        assert (out.message_id, out.body, out.routing_key) == ("m", b"x", "q")
        assert env.headers["x-rabbitkit-retry-count"] == 99  # the caller's envelope is not mutated
        warnings = [r for r in caplog.records if "reserved header" in r.getMessage()]
        assert len(warnings) == 1
        assert "x-my-retry" in warnings[0].getMessage()


# ── broker.publish() ─────────────────────────────────────────────────────


def _config(strip: bool, retry: RetryConfig | None = None) -> RabbitConfig:
    return RabbitConfig(publisher=PublisherConfig(strip_reserved_headers=strip), retry=retry)


class TestSyncBrokerPublish:
    def _broker(self, config: RabbitConfig) -> tuple[SyncBroker, list[MessageEnvelope]]:
        sent: list[MessageEnvelope] = []
        broker = SyncBroker(config)
        transport = MagicMock()

        def publish(env: MessageEnvelope) -> PublishOutcome:
            sent.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        transport.publish = publish
        broker._transport = transport
        return broker, sent

    def test_off_by_default(self) -> None:
        broker, sent = self._broker(RabbitConfig())
        broker.publish(routing_key="q", body=b"", headers=dict(FORGED))
        assert sent[0].headers == FORGED

    def test_strips_application_publishes(self) -> None:
        broker, sent = self._broker(_config(True, RetryConfig(retry_header="x-my-retry")))
        broker.publish(MessageEnvelope(routing_key="q", body=b"", headers={**FORGED, "x-my-retry": 5}))
        assert sent[0].headers == KEPT

    def test_internal_republish_keeps_them(self) -> None:
        broker, sent = self._broker(_config(True))
        broker._flow_controlled_internal_publish(MessageEnvelope(routing_key="q.retry.1", body=b"", headers=FORGED))
        assert sent[0].headers == FORGED


class TestAsyncBrokerPublish:
    async def _broker(self, config: RabbitConfig) -> tuple[AsyncBroker, list[MessageEnvelope]]:
        sent: list[MessageEnvelope] = []

        async def publish(env: MessageEnvelope, **_: Any) -> PublishOutcome:
            sent.append(env)
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        transport = AsyncMock()
        transport.publish = publish
        transport.byte_limiter = None
        broker = AsyncBroker(config)
        with patch("rabbitkit.async_.broker.AsyncTransportImpl", return_value=transport):
            await broker.start(install_signal_handlers=False)
        return broker, sent

    async def test_off_by_default(self) -> None:
        broker, sent = await self._broker(RabbitConfig())
        await broker.publish(routing_key="q", body=b"", headers=dict(FORGED))
        assert sent[0].headers == FORGED

    async def test_strips_application_publishes(self) -> None:
        broker, sent = await self._broker(_config(True))
        await broker.publish(MessageEnvelope(routing_key="q", body=b"", headers=FORGED))
        assert sent[0].headers == KEPT

    async def test_internal_republish_keeps_them(self) -> None:
        broker, sent = await self._broker(_config(True))
        await broker._flow_controlled_internal_publish(
            MessageEnvelope(routing_key="q.retry.1", body=b"", headers=FORGED)
        )
        assert sent[0].headers == FORGED
