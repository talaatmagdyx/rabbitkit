"""Tests for core/route.py — RouteDefinition, ResultPublisher, validation."""

from __future__ import annotations

import pytest

from rabbitkit.core.config import RETRY_DISABLED, RetryConfig
from rabbitkit.core.route import (
    ConfigurationError,
    ResultPublisher,
    RouteDefinition,
    RouteRuntimeState,
)
from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import AckPolicy

# ── helpers ───────────────────────────────────────────────────────────────


def _handler() -> None:
    pass


def _make_route(**kwargs: object) -> RouteDefinition:
    defaults: dict[str, object] = {
        "name": "test-route",
        "queue": RabbitQueue(name="test-queue"),
        "exchange": RabbitExchange(name="test-exchange"),
        "handler": _handler,
    }
    defaults.update(kwargs)
    return RouteDefinition(**defaults)  # type: ignore[arg-type]


# ── ResultPublisher ──────────────────────────────────────────────────────


class TestResultPublisher:
    def test_with_exchange_object(self) -> None:
        ex = RabbitExchange(name="events")
        rp = ResultPublisher(exchange=ex, routing_key="orders.created")
        assert rp.resolve_exchange_name() == "events"
        assert rp.routing_key == "orders.created"

    def test_with_exchange_string(self) -> None:
        rp = ResultPublisher(exchange="events", routing_key="rk")
        assert rp.resolve_exchange_name() == "events"

    def test_with_exchange_none(self) -> None:
        rp = ResultPublisher(exchange=None, routing_key="rk")
        assert rp.resolve_exchange_name() == ""

    def test_defaults(self) -> None:
        rp = ResultPublisher()
        assert rp.exchange is None
        assert rp.routing_key == ""

    def test_frozen(self) -> None:
        rp = ResultPublisher(exchange="events", routing_key="rk")
        with pytest.raises(AttributeError):
            rp.routing_key = "changed"  # type: ignore[misc]


# ── RouteDefinition construction ─────────────────────────────────────────


class TestRouteDefinitionConstruction:
    def test_required_fields(self) -> None:
        route = _make_route()
        assert route.name == "test-route"
        assert route.queue.name == "test-queue"
        assert route.exchange is not None
        assert route.handler is _handler

    def test_defaults(self) -> None:
        route = _make_route()
        assert route.ack_policy == AckPolicy.AUTO
        assert route.route_middlewares == []
        assert route.result_publisher is None
        assert route.serializer_override is None
        assert route.retry_override is None
        assert route.prefetch_count is None
        assert route.tags == frozenset()
        assert route.description == ""
        assert route.consumer_tag is None

    def test_with_result_publisher(self) -> None:
        rp = ResultPublisher(exchange="events", routing_key="rk")
        route = _make_route(result_publisher=rp)
        assert route.result_publisher is rp

    def test_consumer_tag_mutable_via_runtime_state(self) -> None:
        """L10: the only supported write path is route.runtime_state.consumer_tag."""
        route = _make_route()
        assert route.consumer_tag is None
        route.runtime_state.consumer_tag = "ctag.1"
        assert route.consumer_tag == "ctag.1"

    def test_with_tags(self) -> None:
        route = _make_route(tags=frozenset({"orders", "v2"}))
        assert "orders" in route.tags
        assert "v2" in route.tags

    def test_prefetch_count_default_none(self) -> None:
        route = _make_route()
        assert route.prefetch_count is None

    def test_prefetch_count_set(self) -> None:
        route = _make_route(prefetch_count=50)
        assert route.prefetch_count == 50

    def test_frozen_cannot_reassign_metadata(self) -> None:
        route = _make_route()
        with pytest.raises(AttributeError):
            route.name = "other"  # type: ignore[misc]

    def test_runtime_state_default(self) -> None:
        route = _make_route()
        assert isinstance(route.runtime_state, RouteRuntimeState)
        assert route.runtime_state.consumer_tag is None

    def test_consumer_tag_via_runtime_state_is_mutable(self) -> None:
        route = _make_route()
        assert route.consumer_tag is None
        route.runtime_state.consumer_tag = "ctag.1"
        assert route.runtime_state.consumer_tag == "ctag.1"
        # Backward-compat property reflects the runtime state.
        assert route.consumer_tag == "ctag.1"

    def test_consumer_tag_property_is_read_only(self) -> None:
        """L10: no monkey-patched __setattr__ back door -- consumer_tag is
        genuinely read-only on the frozen RouteDefinition. Writing it must
        raise, not silently delegate to runtime_state."""
        route = _make_route()
        with pytest.raises((AttributeError, TypeError)):
            route.consumer_tag = "ctag.2"  # type: ignore[misc]

    def test_consumer_tag_delete_raises(self) -> None:
        """L10: no delegated delete either -- del route.consumer_tag raises."""
        route = _make_route()
        route.runtime_state.consumer_tag = "ctag.3"
        with pytest.raises((AttributeError, TypeError)):
            del route.consumer_tag
        assert route.consumer_tag == "ctag.3"  # unchanged

    def test_default_runtime_state_is_unique_per_instance(self) -> None:
        route_a = _make_route()
        route_b = _make_route()
        assert route_a.runtime_state is not route_b.runtime_state
        route_a.runtime_state.consumer_tag = "a"
        assert route_b.runtime_state.consumer_tag is None

    def test_runtime_state_shared_when_passed_explicitly(self) -> None:
        state = RouteRuntimeState(consumer_tag="pre")
        route = _make_route(runtime_state=state)
        assert route.runtime_state is state
        assert route.consumer_tag == "pre"


# ── Retry resolution ────────────────────────────────────────────────────


class TestRetryResolution:
    def test_no_retry_no_broker(self) -> None:
        route = _make_route()
        assert route.has_retry_enabled() is False
        assert route.effective_retry_config() is None

    def test_inherit_broker_default(self) -> None:
        broker_retry = RetryConfig()
        route = _make_route()
        assert route.has_retry_enabled(broker_retry) is True
        assert route.effective_retry_config(broker_retry) is broker_retry

    def test_per_route_override(self) -> None:
        broker_retry = RetryConfig(max_retries=4)
        route_retry = RetryConfig(max_retries=2)
        route = _make_route(retry_override=route_retry)
        assert route.has_retry_enabled(broker_retry) is True
        assert route.effective_retry_config(broker_retry) is route_retry

    def test_per_route_override_no_broker(self) -> None:
        route_retry = RetryConfig(max_retries=2)
        route = _make_route(retry_override=route_retry)
        assert route.has_retry_enabled() is True
        assert route.effective_retry_config() is route_retry

    def test_explicit_disable(self) -> None:
        broker_retry = RetryConfig()
        route = _make_route(retry_override=RETRY_DISABLED)
        assert route.has_retry_enabled(broker_retry) is False
        assert route.effective_retry_config(broker_retry) is None

    def test_explicit_disable_no_broker(self) -> None:
        route = _make_route(retry_override=RETRY_DISABLED)
        assert route.has_retry_enabled() is False

    def test_retry_disabled_vs_max_retries_zero(self) -> None:
        """RetryConfig(max_retries=0) still enables retry-owned terminal semantics."""
        route_zero = _make_route(retry_override=RetryConfig(max_retries=0))
        route_disabled = _make_route(retry_override=RETRY_DISABLED)

        assert route_zero.has_retry_enabled() is True
        assert route_disabled.has_retry_enabled() is False


# ── Validation: retry + ack policy ───────────────────────────────────────


class TestRetryAckValidation:
    def test_retry_auto_ok(self) -> None:
        route = _make_route(ack_policy=AckPolicy.AUTO, retry_override=RetryConfig())
        route.validate_retry_ack_compatibility()  # no exception

    def test_retry_nack_on_error_ok(self) -> None:
        route = _make_route(ack_policy=AckPolicy.NACK_ON_ERROR, retry_override=RetryConfig())
        route.validate_retry_ack_compatibility()  # no exception

    def test_retry_manual_raises(self) -> None:
        route = _make_route(ack_policy=AckPolicy.MANUAL, retry_override=RetryConfig())
        with pytest.raises(ConfigurationError, match="MANUAL"):
            route.validate_retry_ack_compatibility()

    def test_retry_ack_first_raises(self) -> None:
        route = _make_route(ack_policy=AckPolicy.ACK_FIRST, retry_override=RetryConfig())
        with pytest.raises(ConfigurationError, match="ACK_FIRST"):
            route.validate_retry_ack_compatibility()

    def test_retry_manual_via_broker_default(self) -> None:
        route = _make_route(ack_policy=AckPolicy.MANUAL)
        with pytest.raises(ConfigurationError, match="MANUAL"):
            route.validate_retry_ack_compatibility(broker_retry=RetryConfig())

    def test_no_retry_manual_ok(self) -> None:
        route = _make_route(ack_policy=AckPolicy.MANUAL)
        route.validate_retry_ack_compatibility()  # no exception (no retry)

    def test_disabled_retry_manual_ok(self) -> None:
        route = _make_route(ack_policy=AckPolicy.MANUAL, retry_override=RETRY_DISABLED)
        route.validate_retry_ack_compatibility(broker_retry=RetryConfig())  # no exception


# ── Validation: retry + DLX conflict ─────────────────────────────────────


class TestRetryDLXConflict:
    def test_retry_no_dlx_ok(self) -> None:
        route = _make_route(retry_override=RetryConfig())
        route.validate_retry_dlx_conflict()  # no exception

    def test_retry_with_manual_dlx_raises(self) -> None:
        queue = RabbitQueue(
            name="orders",
            dead_letter_exchange="custom-dlx",
        )
        route = _make_route(queue=queue, retry_override=RetryConfig())
        with pytest.raises(ConfigurationError, match="dead_letter_exchange"):
            route.validate_retry_dlx_conflict()

    def test_retry_with_manual_dlrk_raises(self) -> None:
        queue = RabbitQueue(
            name="orders",
            dead_letter_routing_key="orders.dead",
        )
        route = _make_route(queue=queue, retry_override=RetryConfig())
        with pytest.raises(ConfigurationError, match="dead_letter_routing_key"):
            route.validate_retry_dlx_conflict()

    def test_no_retry_with_dlx_ok(self) -> None:
        queue = RabbitQueue(
            name="orders",
            dead_letter_exchange="custom-dlx",
        )
        route = _make_route(queue=queue)
        route.validate_retry_dlx_conflict()  # no exception (no retry)

    def test_disabled_retry_with_dlx_ok(self) -> None:
        queue = RabbitQueue(
            name="orders",
            dead_letter_exchange="custom-dlx",
        )
        route = _make_route(queue=queue, retry_override=RETRY_DISABLED)
        route.validate_retry_dlx_conflict(broker_retry=RetryConfig())  # no exception

    def test_broker_retry_with_manual_dlx_raises(self) -> None:
        queue = RabbitQueue(
            name="orders",
            dead_letter_exchange="custom-dlx",
        )
        route = _make_route(queue=queue)
        with pytest.raises(ConfigurationError, match="dead_letter_exchange"):
            route.validate_retry_dlx_conflict(broker_retry=RetryConfig())


# ── Full validate ────────────────────────────────────────────────────────


class TestFullValidation:
    def test_valid_route(self) -> None:
        route = _make_route(ack_policy=AckPolicy.AUTO, retry_override=RetryConfig())
        route.validate()  # no exception

    def test_catches_ack_conflict(self) -> None:
        route = _make_route(ack_policy=AckPolicy.MANUAL, retry_override=RetryConfig())
        with pytest.raises(ConfigurationError, match="MANUAL"):
            route.validate()

    def test_catches_dlx_conflict(self) -> None:
        queue = RabbitQueue(name="orders", dead_letter_exchange="dlx")
        route = _make_route(queue=queue, retry_override=RetryConfig())
        with pytest.raises(ConfigurationError, match="dead_letter_exchange"):
            route.validate()

    def test_ack_checked_before_dlx(self) -> None:
        """Ack policy conflict is checked first."""
        queue = RabbitQueue(name="orders", dead_letter_exchange="dlx")
        route = _make_route(
            queue=queue,
            ack_policy=AckPolicy.MANUAL,
            retry_override=RetryConfig(),
        )
        with pytest.raises(ConfigurationError, match="MANUAL"):
            route.validate()

    def test_validate_with_broker_retry(self) -> None:
        route = _make_route(ack_policy=AckPolicy.AUTO)
        route.validate(broker_retry=RetryConfig())  # no exception


class TestHeadersBindingValidation:
    """C4: headers-exchange bindings require bind_arguments with a valid x-match."""

    def _headers_exchange(self) -> RabbitExchange:
        from rabbitkit.core.types import ExchangeType

        return RabbitExchange(name="events.headers", type=ExchangeType.HEADERS)

    def test_headers_exchange_without_bind_arguments_raises(self) -> None:
        route = _make_route(exchange=self._headers_exchange())
        with pytest.raises(ConfigurationError, match="bind_arguments"):
            route.validate()

    def test_headers_exchange_with_invalid_x_match_raises(self) -> None:
        queue = RabbitQueue(name="q", bind_arguments={"x-match": "invalid", "type": "order"})
        route = _make_route(queue=queue, exchange=self._headers_exchange())
        with pytest.raises(ConfigurationError, match="x-match"):
            route.validate()

    @pytest.mark.parametrize("x_match", ["all", "any", "all-with-x", "any-with-x"])
    def test_headers_exchange_with_valid_x_match_passes(self, x_match: str) -> None:
        queue = RabbitQueue(name="q", bind_arguments={"x-match": x_match, "type": "order"})
        route = _make_route(queue=queue, exchange=self._headers_exchange())
        route.validate()  # no exception

    def test_headers_exchange_x_match_defaults_to_all(self) -> None:
        """RabbitMQ defaults a missing x-match to 'all' — accepted."""
        queue = RabbitQueue(name="q", bind_arguments={"type": "order"})
        route = _make_route(queue=queue, exchange=self._headers_exchange())
        route.validate()  # no exception

    def test_non_headers_exchange_needs_no_bind_arguments(self) -> None:
        route = _make_route()  # default direct exchange
        route.validate()  # no exception


class TestRejectWithoutDLXResolution:
    """C3: RouteDefinition.resolve_safety_dlq / can_reject_without_dlx."""

    def _safety(self, **kw: object) -> object:
        from rabbitkit.core.config import SafetyConfig

        return SafetyConfig(**kw)  # type: ignore[arg-type]

    def test_invalid_override_value_raises_at_validation(self) -> None:
        route = _make_route(reject_without_dlx="always")
        with pytest.raises(ConfigurationError, match="reject_without_dlx"):
            route.validate()

    def test_auto_provision_returns_dlq_name(self) -> None:
        from rabbitkit.core.config import SafetyConfig

        route = _make_route(queue=RabbitQueue(name="orders"))
        assert route.resolve_safety_dlq(SafetyConfig()) == "orders.dlq"

    def test_retry_enabled_route_needs_nothing(self) -> None:
        from rabbitkit.core.config import SafetyConfig

        route = _make_route(retry_override=RetryConfig())
        assert route.resolve_safety_dlq(SafetyConfig()) is None

    def test_manual_dlx_respected(self) -> None:
        from rabbitkit.core.config import SafetyConfig

        queue = RabbitQueue(name="orders", dead_letter_exchange="my-dlx")
        route = _make_route(queue=queue)
        assert route.resolve_safety_dlq(SafetyConfig()) is None

    def test_ack_first_without_filter_cannot_reject(self) -> None:
        from rabbitkit.core.config import SafetyConfig

        route = _make_route(ack_policy=AckPolicy.ACK_FIRST)
        assert route.resolve_safety_dlq(SafetyConfig(reject_without_dlx="error")) is None

    def test_ack_first_with_filter_can_reject(self) -> None:
        from rabbitkit.core.config import SafetyConfig

        route = _make_route(ack_policy=AckPolicy.ACK_FIRST, filter_fn=lambda m: True)
        assert route.resolve_safety_dlq(SafetyConfig()) == "test-queue.dlq"

    def test_error_policy_raises_unsafe_topology(self) -> None:
        from rabbitkit.core.config import SafetyConfig
        from rabbitkit.core.errors import UnsafeTopologyError

        route = _make_route()
        with pytest.raises(UnsafeTopologyError, match="dead-letter"):
            route.resolve_safety_dlq(SafetyConfig(reject_without_dlx="error"))

    def test_unsafe_topology_error_is_configuration_error(self) -> None:
        from rabbitkit.core.errors import UnsafeTopologyError

        assert issubclass(UnsafeTopologyError, ConfigurationError)

    def test_discard_policy_warns_and_returns_none(self) -> None:
        from rabbitkit.core.config import SafetyConfig

        route = _make_route()
        with pytest.warns(RuntimeWarning, match="permanently discarded"):
            result = route.resolve_safety_dlq(SafetyConfig(reject_without_dlx="discard"))
        assert result is None

    def test_discard_policy_silent_when_warn_disabled(self) -> None:
        import warnings

        from rabbitkit.core.config import SafetyConfig

        route = _make_route()
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            result = route.resolve_safety_dlq(
                SafetyConfig(reject_without_dlx="discard", warn_on_discard=False)
            )
        assert result is None
        assert caught == []

    def test_per_route_override_beats_config(self) -> None:
        from rabbitkit.core.config import SafetyConfig
        from rabbitkit.core.errors import UnsafeTopologyError

        route = _make_route(reject_without_dlx="error")
        with pytest.raises(UnsafeTopologyError):
            route.resolve_safety_dlq(SafetyConfig())  # config default auto_provision

    def test_enum_member_accepted_as_override(self) -> None:
        from rabbitkit.core.config import SafetyConfig
        from rabbitkit.core.types import RejectWithoutDLXPolicy

        route = _make_route(reject_without_dlx=RejectWithoutDLXPolicy.AUTO_PROVISION)
        route.validate()
        assert route.resolve_safety_dlq(SafetyConfig(reject_without_dlx="discard")) == "test-queue.dlq"


class TestRouteDynamic:
    def test_delete_consumer_tag_raises(self) -> None:
        """L10: consumer_tag has no deleter -- del route.consumer_tag raises.
        Resetting it is done via route.runtime_state.consumer_tag = None."""
        route = _make_route()
        route.runtime_state.consumer_tag = "my-tag"
        with pytest.raises((AttributeError, TypeError)):
            del route.consumer_tag
        assert route.consumer_tag == "my-tag"  # unchanged
        route.runtime_state.consumer_tag = None
        assert route.consumer_tag is None

    def test_delete_other_field_raises(self) -> None:
        """del on any field other than consumer_tag raises FrozenInstanceError."""
        from dataclasses import FrozenInstanceError
        route = _make_route()
        with pytest.raises(FrozenInstanceError):
            del route.name  # type: ignore[misc]
