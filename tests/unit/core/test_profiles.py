"""Tests for core/profiles.py — reliability profiles, preflight, policy templates."""

from __future__ import annotations

import dataclasses
import json
from typing import Any

import pytest

from rabbitkit.core.config import (
    RETRY_DISABLED,
    ConnectionConfig,
    ConsumerConfig,
    PublisherConfig,
    RabbitConfig,
    RetryConfig,
    RetryDisabled,
    SafetyConfig,
    SecurityConfig,
    SSLConfig,
)
from rabbitkit.core.errors import ConfigValidationError
from rabbitkit.core.profiles import (
    CRITICAL_MAX_IN_FLIGHT_BYTES,
    CRITICAL_MAX_MESSAGE_BYTES,
    PolicyTemplate,
    PreflightReport,
    ProfileViolation,
    apply_profile,
    critical_config,
    policy_templates,
    preflight,
    standard_config,
    validate_profile,
)
from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import (
    AckPolicy,
    DeadLetterStrategy,
    PreflightStatus,
    QueueType,
    ReliabilityProfile,
    TerminalAction,
)


class _Route:
    def __init__(
        self,
        queue: RabbitQueue,
        *,
        name: str = "r",
        ack_policy: AckPolicy = AckPolicy.AUTO,
        retry_override: RetryConfig | RetryDisabled | None = None,
    ) -> None:
        self.queue = queue
        self.name = name
        self.ack_policy = ack_policy
        self.retry_override = retry_override


def _critical_retry(**kw: Any) -> RetryConfig:
    base: dict[str, Any] = {
        "delay_queue_type": "quorum",
        "dlq_queue_type": "quorum",
        "delay_queue_dead_letter_strategy": "at-least-once",
        "trust_retry_header": False,
    }
    base.update(kw)
    return RetryConfig(**base)


def _critical_ok_config() -> RabbitConfig:
    return RabbitConfig(
        publisher=PublisherConfig(
            mandatory=True,
            max_message_bytes=CRITICAL_MAX_MESSAGE_BYTES,
            max_in_flight_bytes=CRITICAL_MAX_IN_FLIGHT_BYTES,
            strip_reserved_headers=True,
        ),
        consumer=ConsumerConfig(processing_timeout=30.0),
        retry=_critical_retry(),
        safety=SafetyConfig(dead_letter_with_metadata=True),
    )


def _quorum_alo(name: str) -> RabbitQueue:
    return RabbitQueue(
        name=name,
        queue_type=QueueType.QUORUM,
        dead_letter_strategy=DeadLetterStrategy.AT_LEAST_ONCE,
        overflow="reject-publish",
    )


_TIMEOUT = RabbitConfig(consumer=ConsumerConfig(processing_timeout=30.0))


class TestValidateProfile:
    def test_default_config_is_standard_compliant_except_mandatory_warning(self) -> None:
        v = validate_profile(RabbitConfig(), ReliabilityProfile.STANDARD)
        assert [x.setting for x in v] == ["publisher.mandatory"]
        assert v[0].severity == "warning"
        assert "publisher.mandatory" in str(v[0])

    def test_standard_flags_unsafe_settings(self) -> None:
        cfg = RabbitConfig(
            publisher=PublisherConfig(confirm_delivery=False, persistent=False, max_message_bytes=0),
            safety=SafetyConfig(reject_without_dlx="discard"),
            retry=RetryConfig(error_detail="raw"),
        )
        settings = {x.setting for x in validate_profile(cfg, "standard")}
        assert settings >= {
            "publisher.confirm_delivery",
            "publisher.persistent",
            "publisher.max_message_bytes",
            "safety.reject_without_dlx",
            "retry.error_detail",
        }

    def test_critical_requires_retry_and_quorum_chain(self) -> None:
        v = validate_profile(RabbitConfig(publisher=PublisherConfig(mandatory=True)), ReliabilityProfile.CRITICAL)
        settings = {x.setting for x in v}
        assert "retry" in settings
        assert "publisher.max_message_bytes" in settings  # 16 MiB default > 256 KiB

    def test_critical_classic_delay_chain_flagged(self) -> None:
        cfg = RabbitConfig(
            publisher=PublisherConfig(mandatory=True, max_message_bytes=1024),
            retry=RetryConfig(delay_queue_type="classic", dlq_queue_type="classic"),
        )
        settings = {x.setting for x in validate_profile(cfg, ReliabilityProfile.CRITICAL)}
        assert {"retry.delay_queue_type", "retry.dlq_queue_type"} <= settings

    def test_critical_compliant(self) -> None:
        assert validate_profile(_critical_ok_config(), ReliabilityProfile.CRITICAL) == []

    def test_critical_route_queues_must_be_quorum(self) -> None:
        routes = [_Route(RabbitQueue(name="orders")), _Route(_quorum_alo("pay"))]
        v = validate_profile(_critical_ok_config(), ReliabilityProfile.CRITICAL, routes=routes)
        assert [x.setting for x in v] == ["queue[orders].queue_type"]

    def test_critical_quorum_queue_without_at_least_once_is_a_warning(self) -> None:
        routes = [_Route(RabbitQueue(name="pay", queue_type=QueueType.QUORUM))]
        (v,) = validate_profile(_critical_ok_config(), ReliabilityProfile.CRITICAL, routes=routes)
        assert v.setting == "queue[pay].dead_letter_strategy" and v.severity == "warning"

    def test_critical_at_least_once_via_arguments_counts(self) -> None:
        q = RabbitQueue(
            name="pay",
            queue_type=QueueType.QUORUM,
            arguments={"x-dead-letter-strategy": "at-least-once", "x-overflow": "reject-publish"},
        )
        assert validate_profile(_critical_ok_config(), ReliabilityProfile.CRITICAL, routes=[_Route(q)]) == []

    def test_critical_rejects_ack_first_and_disabled_retry_on_routes(self) -> None:
        routes = [
            _Route(_quorum_alo("a"), name="a", ack_policy=AckPolicy.ACK_FIRST),
            _Route(_quorum_alo("b"), name="b", retry_override=RETRY_DISABLED),
        ]
        v = validate_profile(_critical_ok_config(), ReliabilityProfile.CRITICAL, routes=routes)
        assert {(x.setting, x.severity) for x in v} == {("route[a].ack_policy", "error"), ("route[b].retry", "error")}

    def test_ack_first_is_allowed_by_standard(self) -> None:
        routes = [_Route(RabbitQueue(name="a"), ack_policy=AckPolicy.ACK_FIRST)]
        assert validate_profile(standard_config(), ReliabilityProfile.STANDARD, routes=routes) == []

    def test_critical_checks_a_route_retry_override(self) -> None:
        route = _Route(_quorum_alo("a"), name="a", retry_override=RetryConfig(error_detail="raw"))
        settings = {x.setting for x in validate_profile(_critical_ok_config(), "critical", routes=[route])}
        assert settings == {
            "route[a].retry.delay_queue_type",
            "route[a].retry.delay_queue_dead_letter_strategy",
            "route[a].retry.trust_retry_header",
            "route[a].retry.error_detail",
        }

    def test_critical_requires_the_consumer_and_publisher_guards(self) -> None:
        cfg = dataclasses.replace(
            _critical_ok_config(),
            publisher=PublisherConfig(mandatory=True, max_message_bytes=1024),
            consumer=ConsumerConfig(reject_transient_on_redelivery=False, manual_settle_on_error=False),
            retry=_critical_retry(terminal_action=TerminalAction.REJECT, trust_retry_header=True),
        )
        v = {x.setting: x for x in validate_profile(cfg, ReliabilityProfile.CRITICAL)}
        assert set(v) == {
            "publisher.max_in_flight_bytes",
            "publisher.strip_reserved_headers",
            "consumer.processing_timeout",
            "consumer.reject_transient_on_redelivery",
            "consumer.manual_settle_on_error",
            "retry.terminal_action",
            "retry.trust_retry_header",
        }
        assert "no safe default" in v["consumer.processing_timeout"].expected

    def test_critical_inherited_classic_dlq_flagged(self) -> None:
        cfg = RabbitConfig(
            publisher=PublisherConfig(mandatory=True, max_message_bytes=1024),
            retry=RetryConfig(delay_queue_type="quorum", dlq_queue_type="inherit"),
        )
        routes = [_Route(RabbitQueue(name="orders"))]
        settings = {x.setting for x in validate_profile(cfg, ReliabilityProfile.CRITICAL, routes=routes)}
        assert "queue[orders].dlq" in settings


class TestCriticalConnection:
    """CRITICAL requires heartbeats; the reconnect budget and TLS stay the
    deployment's choice, checked only for consistency (warnings)."""

    @staticmethod
    def _with(connection: ConnectionConfig | None = None, security: SecurityConfig | None = None) -> RabbitConfig:
        cfg = _critical_ok_config()
        if connection is not None:
            cfg = dataclasses.replace(cfg, connection=connection)
        if security is not None:
            cfg = dataclasses.replace(cfg, security=security)
        return cfg

    def test_defaults_meet_the_connection_requirements(self) -> None:
        assert validate_profile(self._with(), "critical") == []

    def test_heartbeat_off_is_an_error(self) -> None:
        with pytest.warns(UserWarning, match="heartbeat=0"):
            conn = ConnectionConfig(heartbeat=0)
        v = validate_profile(self._with(conn), "critical")
        assert [(x.setting, x.severity) for x in v] == [("connection.heartbeat", "error")]

    def test_apply_refuses_heartbeat_off(self) -> None:
        with pytest.warns(UserWarning, match="heartbeat=0"):
            conn = ConnectionConfig(heartbeat=0)
        base = dataclasses.replace(_TIMEOUT, connection=conn)
        with pytest.raises(ConfigValidationError, match=r"connection.heartbeat=0"):
            critical_config(base)

    def test_standard_does_not_require_heartbeats(self) -> None:
        with pytest.warns(UserWarning, match="heartbeat=0"):
            conn = ConnectionConfig(heartbeat=0)
        assert standard_config(RabbitConfig(connection=conn)).connection.heartbeat == 0

    def test_unlimited_reconnect_budget_is_the_default_and_compliant(self) -> None:
        cfg = critical_config(_TIMEOUT)
        assert cfg.connection.reconnect_budget_attempts is None
        assert cfg.connection.reconnect_budget_seconds is None

    def test_budget_shorter_than_the_backoff_cap_warns(self) -> None:
        conn = ConnectionConfig(reconnect_backoff_max=30.0, reconnect_budget_seconds=10.0)
        v = validate_profile(self._with(conn), "critical")
        assert [(x.setting, x.severity) for x in v] == [("connection.reconnect_budget_seconds", "warning")]
        assert critical_config(dataclasses.replace(_TIMEOUT, connection=conn)).connection == conn

    def test_bounded_budgets_that_cover_the_backoff_are_compliant(self) -> None:
        conn = ConnectionConfig(reconnect_budget_attempts=20, reconnect_budget_seconds=300.0)
        assert validate_profile(self._with(conn), "critical") == []

    def test_tls_is_not_required(self) -> None:
        assert validate_profile(self._with(security=SecurityConfig(ssl=SSLConfig(enabled=False))), "critical") == []

    def test_tls_without_certificate_verification_warns(self) -> None:
        security = SecurityConfig(ssl=SSLConfig(enabled=True, cert_reqs="CERT_NONE"))
        v = validate_profile(self._with(security=security), "critical")
        assert [(x.setting, x.severity) for x in v] == [("security.ssl.cert_reqs", "warning")]

    def test_tls_with_verification_is_compliant(self) -> None:
        security = SecurityConfig(ssl=SSLConfig(enabled=True))
        assert validate_profile(self._with(security=security), "critical") == []


class TestApplyProfile:
    def test_standard_fills_in_guardrails(self) -> None:
        cfg = standard_config()
        assert cfg.publisher.confirm_delivery and cfg.publisher.persistent and cfg.publisher.mandatory
        assert cfg.retry is None  # standard does not force retry
        assert validate_profile(cfg, ReliabilityProfile.STANDARD) == []

    def test_critical_fills_in_requirements(self) -> None:
        cfg = critical_config(_TIMEOUT)
        assert cfg.publisher.max_message_bytes == CRITICAL_MAX_MESSAGE_BYTES
        assert cfg.publisher.max_in_flight_bytes == CRITICAL_MAX_IN_FLIGHT_BYTES
        assert cfg.publisher.strip_reserved_headers is True
        assert cfg.retry is not None
        assert cfg.retry.delay_queue_type == "quorum" and cfg.retry.dlq_queue_type == "quorum"
        assert cfg.retry.delay_queue_dead_letter_strategy == "at-least-once"
        assert cfg.retry.terminal_action is TerminalAction.DEAD_LETTER
        assert cfg.retry.trust_retry_header is False
        assert cfg.consumer.reject_transient_on_redelivery and cfg.consumer.manual_settle_on_error
        assert cfg.safety.dead_letter_with_metadata is True
        assert validate_profile(cfg, ReliabilityProfile.CRITICAL) == []

    def test_critical_requires_dead_letter_metadata_copies(self) -> None:
        cfg = dataclasses.replace(_critical_ok_config(), safety=SafetyConfig())
        v = validate_profile(cfg, ReliabilityProfile.CRITICAL)
        assert [(x.setting, x.severity) for x in v] == [("safety.dead_letter_with_metadata", "error")]
        assert validate_profile(cfg, ReliabilityProfile.STANDARD) == []

    def test_critical_requires_a_processing_timeout(self) -> None:
        with pytest.raises(ConfigValidationError, match=r"processing_timeout is required.*no safe default"):
            critical_config()

    def test_critical_keeps_a_user_in_flight_byte_budget(self) -> None:
        base = dataclasses.replace(_TIMEOUT, publisher=PublisherConfig(max_in_flight_bytes=32 * 1024 * 1024))
        assert apply_profile(base, "critical").publisher.max_in_flight_bytes == 32 * 1024 * 1024

    def test_critical_keeps_tighter_user_body_limit(self) -> None:
        base = dataclasses.replace(_TIMEOUT, publisher=PublisherConfig(max_message_bytes=1024))
        cfg = apply_profile(base, "critical")
        assert cfg.publisher.max_message_bytes == 1024

    def test_critical_preserves_user_retry_ladder(self) -> None:
        base = dataclasses.replace(_TIMEOUT, retry=RetryConfig(max_retries=2, delays=(1, 2)))
        cfg = apply_profile(base, ReliabilityProfile.CRITICAL)
        assert cfg.retry is not None and cfg.retry.max_retries == 2 and cfg.retry.delays == (1, 2)

    def test_does_not_mutate_input(self) -> None:
        base = _TIMEOUT
        apply_profile(base, ReliabilityProfile.CRITICAL)
        assert base.publisher.mandatory is False
        assert base.publisher.strip_reserved_headers is False

    def test_standard_leaves_the_critical_only_settings_alone(self) -> None:
        cfg = standard_config(RabbitConfig(retry=RetryConfig()))
        assert cfg.publisher.max_in_flight_bytes is None and cfg.publisher.strip_reserved_headers is False
        assert cfg.retry == RetryConfig()
        assert cfg.consumer.processing_timeout is None

    @pytest.mark.parametrize(
        ("change", "fragment"),
        [
            ({"retry": RetryConfig(terminal_action=TerminalAction.REJECT)}, "terminal_action='reject'"),
            (
                {"retry": RetryConfig(delay_queue_type="quorum", delay_queue_dead_letter_strategy="at-most-once")},
                "delay_queue_dead_letter_strategy='at-most-once'",
            ),
            (
                {"consumer": ConsumerConfig(processing_timeout=5, reject_transient_on_redelivery=False)},
                "reject_transient_on_redelivery=False",
            ),
            (
                {"consumer": ConsumerConfig(processing_timeout=5, manual_settle_on_error=False)},
                "manual_settle_on_error=False",
            ),
        ],
    )
    def test_critical_pinned_contradictions(self, change: dict[str, Any], fragment: str) -> None:
        with pytest.raises(ConfigValidationError, match=fragment.replace("(", "\\(").replace(")", "\\)")):
            apply_profile(dataclasses.replace(_TIMEOUT, **change), "critical")
        # STANDARD accepts the same settings.
        apply_profile(dataclasses.replace(_TIMEOUT, **change), "standard")

    @pytest.mark.parametrize(
        ("cfg", "fragment"),
        [
            (RabbitConfig(publisher=PublisherConfig(confirm_delivery=False)), "confirm_delivery=False"),
            (RabbitConfig(publisher=PublisherConfig(persistent=False)), "persistent=False"),
            (RabbitConfig(safety=SafetyConfig(reject_without_dlx="discard")), "reject_without_dlx='discard'"),
            (RabbitConfig(retry=RetryConfig(error_detail="raw")), "error_detail='raw'"),
        ],
    )
    def test_contradictions_raise_for_both_profiles(self, cfg: RabbitConfig, fragment: str) -> None:
        for prof in ReliabilityProfile:
            with pytest.raises(ConfigValidationError, match=fragment.replace("(", "\\(").replace(")", "\\)")):
                apply_profile(cfg, prof)

    def test_critical_contradictions(self) -> None:
        with pytest.raises(ConfigValidationError, match="max_message_bytes=0"):
            apply_profile(RabbitConfig(publisher=PublisherConfig(max_message_bytes=0)), "critical")
        with pytest.raises(ConfigValidationError, match="> 256 KiB"):
            apply_profile(RabbitConfig(publisher=PublisherConfig(max_message_bytes=10_000_000)), "critical")
        with pytest.raises(ConfigValidationError, match="dlq_queue_type='classic'"):
            apply_profile(RabbitConfig(retry=RetryConfig(dlq_queue_type="classic")), "critical")
        # default 16 MiB is NOT pinned → gets lowered, no error
        assert apply_profile(_TIMEOUT, "critical").publisher.max_message_bytes == CRITICAL_MAX_MESSAGE_BYTES

    def test_critical_explicit_classic_delay_chain_is_contradiction(self) -> None:
        # explicit "classic" equals the default, so it is treated as unpinned and upgraded
        base = dataclasses.replace(_TIMEOUT, retry=RetryConfig(delay_queue_type="classic"))
        cfg = apply_profile(base, "critical")
        assert cfg.retry is not None and cfg.retry.delay_queue_type == "quorum"


class TestPreflight:
    def test_local_only_reports_unverified_broker_checks(self) -> None:
        routes = [_Route(_quorum_alo("orders"))]
        report = preflight(_critical_ok_config(), ReliabilityProfile.CRITICAL, routes=routes)
        assert report.ok
        assert not report.fully_verified
        names = [c.name for c in report.unverified]
        assert names == ["broker:orders"]
        assert "no management client" in report.unverified[0].detail
        d = report.as_dict()
        assert d["profile"] == "critical" and d["ok"] is True and d["fully_verified"] is False

    def test_config_violations_are_failed(self) -> None:
        report = preflight(RabbitConfig(), ReliabilityProfile.CRITICAL)
        assert not report.ok
        assert all(c.name.startswith("config:") for c in report.failed)

    def test_warning_violation_is_unverified_not_failed(self) -> None:
        report = preflight(RabbitConfig(), ReliabilityProfile.STANDARD)
        assert report.ok
        assert [c.name for c in report.unverified] == ["config:publisher.mandatory"]

    def test_management_client_verified(self) -> None:
        class Mgmt:
            def get_queue(self, name: str, vhost: str = "/") -> dict[str, Any]:
                return {
                    "name": name,
                    "type": "quorum",
                    "arguments": {"x-delivery-limit": 5},
                    "effective_policy_definition": {
                        "dead-letter-strategy": "at-least-once",
                        "overflow": "reject-publish",
                    },
                }

        # Declared plain quorum: the broker check (policy) supersedes the local warning.
        routes = [_Route(RabbitQueue(name="orders", queue_type=QueueType.QUORUM))]
        report = preflight(_critical_ok_config(), "critical", routes=routes, management_client=Mgmt())
        assert report.fully_verified
        assert {c.name for c in report.checks} == {
            "config",
            "broker:orders:type",
            "broker:orders:dead-letter-strategy",
            "broker:orders:overflow",
            "broker:orders:delivery-limit",
        }

    def test_management_client_failed_checks(self) -> None:
        class Mgmt:
            def get_queue(self, name: str, vhost: str = "/") -> dict[str, Any]:
                return {"name": name, "type": "classic", "arguments": {}, "effective_policy_definition": {}}

        routes = [_Route(RabbitQueue(name="orders", queue_type=QueueType.QUORUM))]
        report = preflight(_critical_ok_config(), "critical", routes=routes, management_client=Mgmt())
        assert not report.ok
        failed = {c.name for c in report.failed}
        assert "broker:orders:type" in failed and "broker:orders:dead-letter-strategy" in failed

    def test_management_lookup_error_is_unverified(self) -> None:
        class Mgmt:
            def get_queue(self, name: str, vhost: str = "/") -> dict[str, Any]:
                raise ConnectionError("404")

        routes = [_Route(_quorum_alo("orders"))]
        report = preflight(_critical_ok_config(), "critical", routes=routes, management_client=Mgmt())
        assert report.ok and not report.fully_verified
        assert "ConnectionError" in report.unverified[0].detail

    def test_unexpected_payload_is_unverified(self) -> None:
        class Mgmt:
            def get_queue(self, name: str, vhost: str = "/") -> Any:
                return "not json"

        routes = [_Route(_quorum_alo("orders"))]
        report = preflight(_critical_ok_config(), "critical", routes=routes, management_client=Mgmt())
        assert report.unverified and report.ok

    def test_standard_profile_skips_broker_checks(self) -> None:
        routes = [_Route(RabbitQueue(name="orders"))]
        report = preflight(standard_config(), "standard", routes=routes)
        assert report.fully_verified

    def test_report_dataclass_defaults(self) -> None:
        assert PreflightReport(profile=ReliabilityProfile.STANDARD).fully_verified
        assert PreflightStatus("verified") is PreflightStatus.VERIFIED


class TestPolicyTemplates:
    def test_critical_templates(self) -> None:
        retry = RetryConfig(max_retries=3, delays=(1, 2, 3))
        tpls = policy_templates(["orders"], retry=retry, profile="critical")
        by_name = {t.name: t for t in tpls}
        assert set(by_name) == {"rabbitkit-orders-source", "rabbitkit-orders-retry", "rabbitkit-orders-dlq"}
        src = by_name["rabbitkit-orders-source"]
        assert src.definition == {
            "overflow": "reject-publish",
            "dead-letter-strategy": "at-least-once",
            "delivery-limit": 4,
        }
        assert src.apply_to == "quorum_queues"
        assert src.pattern == "^orders$"
        assert by_name["rabbitkit-orders-retry"].pattern == r"^orders\.retry\.[0-9]+(\.s[0-9]+)?$"
        assert by_name["rabbitkit-orders-retry"].definition == {"overflow": "reject-publish"}
        assert by_name["rabbitkit-orders-dlq"].pattern == r"^orders\.dlq$"

    def test_standard_templates_only_overflow(self) -> None:
        (src,) = policy_templates(["a.b"], profile=ReliabilityProfile.STANDARD)
        assert src.definition == {"overflow": "reject-publish"}
        assert src.apply_to == "queues"
        assert src.pattern == r"^a\.b$"

    def test_delivery_limit_and_max_length_overrides(self) -> None:
        tpls = policy_templates(["q"], retry=RetryConfig(), profile="critical", delivery_limit=9, max_length=1000)
        src = next(t for t in tpls if t.name.endswith("-source"))
        assert src.definition["delivery-limit"] == 9 and src.definition["max-length"] == 1000
        dlq = next(t for t in tpls if t.name.endswith("-dlq"))
        assert dlq.definition["max-length"] == 1000

    def test_renderers(self) -> None:
        t = PolicyTemplate(name="p", pattern="^q$", definition={"overflow": "reject-publish"}, vhost="prod")
        body = t.as_api_body()
        assert body == {
            "pattern": "^q$",
            "definition": {"overflow": "reject-publish"},
            "apply-to": "quorum_queues",
            "priority": 10,
        }
        cmd = t.as_rabbitmqctl()
        assert cmd.startswith("rabbitmqctl set_policy -p prod --priority 10 --apply-to quorum_queues p '^q$' ")
        assert json.loads(cmd.split("' '")[-1].rstrip("'")) == {"overflow": "reject-publish"}

    def test_violation_str(self) -> None:
        assert str(ProfileViolation("a", "b", "c")) == "a: expected b, got c (error)"
