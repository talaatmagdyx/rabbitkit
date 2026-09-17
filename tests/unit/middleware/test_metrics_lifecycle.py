"""Plan §11: previously declared-but-never-emitted metrics — in-flight gauge and
publish confirm latency — plus the new bulk/settlement metric names."""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock

import pytest

from rabbitkit.core.config import MetricsConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.middleware.metrics import MetricsMiddleware


def _msg() -> RabbitMessage:
    return RabbitMessage(body=b"x", headers={"x-rabbitkit-original-queue": "orders"}, routing_key="orders")


class TestInFlightGauge:
    def test_sync_gauge_up_and_down(self) -> None:
        collector = MagicMock()
        mw = MetricsMiddleware(collector=collector)
        observed: list[float] = []

        def handler(m: RabbitMessage) -> str:
            observed.append(
                next(
                    c.args[2] for c in collector.set_gauge.call_args_list if c.args[0] == "rabbitkit_in_flight_messages"
                )
            )
            return "ok"

        assert mw.consume_scope(handler, _msg()) == "ok"
        assert observed == [1.0]
        gauge_calls = [
            c.args for c in collector.set_gauge.call_args_list if c.args[0] == "rabbitkit_in_flight_messages"
        ]
        assert gauge_calls[-1] == ("rabbitkit_in_flight_messages", {"queue": "orders"}, 0.0)

    def test_sync_gauge_decrements_on_error(self) -> None:
        collector = MagicMock()
        mw = MetricsMiddleware(collector=collector)

        def handler(m: RabbitMessage) -> None:
            raise ValueError("x")

        with pytest.raises(ValueError):
            mw.consume_scope(handler, _msg())
        last = [c.args for c in collector.set_gauge.call_args_list if c.args[0] == "rabbitkit_in_flight_messages"][-1]
        assert last[2] == 0.0

    async def test_async_gauge(self) -> None:
        collector = MagicMock()
        mw = MetricsMiddleware(collector=collector)

        async def handler(m: RabbitMessage) -> str:
            return "ok"

        await mw.consume_scope_async(handler, _msg())
        values = [c.args[2] for c in collector.set_gauge.call_args_list if c.args[0] == "rabbitkit_in_flight_messages"]
        assert values == [1.0, 0.0]

    def test_no_collector_is_noop(self) -> None:
        mw = MetricsMiddleware(collector=None)
        assert mw.consume_scope(lambda m: "ok", _msg()) == "ok"

    def test_collector_without_set_gauge_tolerated(self) -> None:
        class Minimal:
            def inc_counter(self, name: str, labels: dict[str, str], value: float = 1.0) -> None: ...

            def observe_histogram(self, name: str, labels: dict[str, str], value: float) -> None: ...

        mw = MetricsMiddleware(collector=Minimal())  # type: ignore[arg-type]
        assert mw.consume_scope(lambda m: "ok", _msg()) == "ok"


class TestConfirmLatency:
    def test_confirmed_observes_latency(self) -> None:
        collector = MagicMock()
        mw = MetricsMiddleware(collector=collector)
        env = MessageEnvelope(routing_key="q", body=b"x", exchange="ex")
        mw.publish_scope(lambda e: PublishOutcome(status=PublishStatus.CONFIRMED), env)
        names = [c.args[0] for c in collector.observe_histogram.call_args_list]
        assert "rabbitkit_publish_confirm_latency_seconds" in names
        assert "rabbitkit_message_publish_seconds" in names
        call = next(
            c for c in collector.observe_histogram.call_args_list if c.args[0].endswith("confirm_latency_seconds")
        )
        assert call.args[1] == {"exchange": "ex"}

    @pytest.mark.parametrize("status", [PublishStatus.SENT, PublishStatus.NACKED, PublishStatus.TIMEOUT])
    def test_non_confirmed_does_not_observe_latency(self, status: PublishStatus) -> None:
        collector = MagicMock()
        mw = MetricsMiddleware(collector=collector)
        env = MessageEnvelope(routing_key="q", body=b"x")
        mw.publish_scope(lambda e: PublishOutcome(status=status), env)
        names = [c.args[0] for c in collector.observe_histogram.call_args_list]
        assert "rabbitkit_publish_confirm_latency_seconds" not in names

    async def test_async_confirmed_observes_latency(self) -> None:
        collector = MagicMock()
        mw = MetricsMiddleware(collector=collector)
        env = MessageEnvelope(routing_key="q", body=b"x")

        async def pub(e: MessageEnvelope) -> PublishOutcome:
            return PublishOutcome(status=PublishStatus.CONFIRMED)

        await mw.publish_scope_async(pub, env)
        names = [c.args[0] for c in collector.observe_histogram.call_args_list]
        assert "rabbitkit_publish_confirm_latency_seconds" in names

    def test_raised_publish_still_counts_error(self) -> None:
        collector = MagicMock()
        mw = MetricsMiddleware(collector=collector)
        env = MessageEnvelope(routing_key="q", body=b"x")

        def boom(e: MessageEnvelope) -> Any:
            raise RuntimeError("x")

        with pytest.raises(RuntimeError):
            mw.publish_scope(boom, env)
        collector.inc_counter.assert_any_call(
            "rabbitkit_messages_published_total", {"exchange": "default", "status": "error"}
        )


class TestNewMetricNames:
    def test_names_derive_from_namespace(self) -> None:
        cfg = MetricsConfig(namespace="svc")
        assert cfg.bulk_publish_items_total == "svc_bulk_publish_items_total"
        assert cfg.bulk_publish_batch_size == "svc_bulk_publish_batch_size"
        assert cfg.bulk_publish_admission_wait_seconds == "svc_bulk_publish_admission_wait_seconds"
        assert cfg.settlement_items_total == "svc_settlement_items_total"
        assert cfg.settlement_coalesced_total == "svc_settlement_coalesced_total"
        assert cfg.retry_handoff_failures_total == "svc_retry_handoff_failures_total"
        assert cfg.retry_handoff_paused == "svc_retry_handoff_paused"
        # the previously-declared lifecycle gauges
        assert cfg.broker_connected == "svc_broker_connected"
        assert cfg.consumer_active == "svc_consumer_active"
        assert cfg.in_flight_messages == "svc_in_flight_messages"
        assert cfg.worker_pool_pending == "svc_worker_pool_pending"
        assert cfg.publish_confirm_latency_seconds == "svc_publish_confirm_latency_seconds"
