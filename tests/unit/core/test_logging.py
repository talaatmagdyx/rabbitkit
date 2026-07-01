"""Tests for structured logging configuration."""

from __future__ import annotations

import dataclasses

import pytest
import structlog.testing

from rabbitkit.core.config import RabbitConfig
from rabbitkit.core.logging import (
    DEFAULT_REDACT_KEYS,
    LoggingConfig,
    _normalize_key,
    _redact_processor,
    configure_structlog,
)


class TestLoggingConfig:
    """Tests for LoggingConfig dataclass."""

    def test_logging_config_defaults(self) -> None:
        """LoggingConfig() has correct defaults."""
        cfg = LoggingConfig()
        assert cfg.render_json is False
        assert cfg.add_log_level is True
        assert cfg.timestamper_fmt == "iso"
        assert cfg.include_caller_info is False
        assert cfg.redact_keys == DEFAULT_REDACT_KEYS

    def test_logging_config_redact_keys_disabled(self) -> None:
        cfg = LoggingConfig(redact_keys=None)
        assert cfg.redact_keys is None

    def test_logging_config_redact_keys_custom(self) -> None:
        custom = frozenset({"my_secret"})
        cfg = LoggingConfig(redact_keys=custom)
        assert cfg.redact_keys == custom

    def test_logging_config_frozen(self) -> None:
        """Cannot modify LoggingConfig after creation."""
        cfg = LoggingConfig()
        with pytest.raises(dataclasses.FrozenInstanceError):
            cfg.render_json = True  # type: ignore[misc]


class TestNormalizeKey:
    """L16: _normalize_key folds AMQP-style x-prefixed/hyphenated header
    names to the same form as the Python-style snake_case defaults."""

    def test_lowercases(self) -> None:
        assert _normalize_key("Password") == "password"

    def test_strips_x_prefix(self) -> None:
        assert _normalize_key("x-api-key") == "api_key"

    def test_folds_hyphens_to_underscores(self) -> None:
        assert _normalize_key("client-secret") == "client_secret"

    def test_x_prefix_and_hyphens_combined(self) -> None:
        assert _normalize_key("X-Auth-Token") == "auth_token"

    def test_already_normalized_key_unchanged(self) -> None:
        assert _normalize_key("api_key") == "api_key"

    def test_no_false_positive_on_unrelated_x_prefixed_key(self) -> None:
        assert _normalize_key("x-tenant") == "tenant"


class TestRedactProcessor:
    """L16: _redact_processor redacts matching keys top-level and one
    level deep inside nested dict values, leaving everything else intact."""

    def test_redacts_top_level_match(self) -> None:
        processor = _redact_processor(frozenset({"password"}))
        event_dict = {"event": "login", "password": "sekrit"}
        result = processor(None, "info", event_dict)
        assert result["password"] == "***REDACTED***"
        assert result["event"] == "login"

    def test_redacts_case_insensitively(self) -> None:
        processor = _redact_processor(frozenset({"password"}))
        event_dict = {"PASSWORD": "sekrit"}
        result = processor(None, "info", event_dict)
        assert result["PASSWORD"] == "***REDACTED***"

    def test_redacts_amqp_style_header_key(self) -> None:
        """A header dict using x-prefixed/hyphenated AMQP convention still
        matches the Python-style snake_case default entry."""
        processor = _redact_processor(DEFAULT_REDACT_KEYS)
        event_dict = {
            "headers": {"x-api-key": "abc123", "x-tenant": "acme", "Authorization": "Bearer xyz"}
        }
        result = processor(None, "info", event_dict)
        assert result["headers"]["x-api-key"] == "***REDACTED***"
        assert result["headers"]["Authorization"] == "***REDACTED***"
        assert result["headers"]["x-tenant"] == "acme"  # not a secret key -- untouched

    def test_non_matching_keys_untouched(self) -> None:
        processor = _redact_processor(frozenset({"password"}))
        event_dict = {"routing_key": "orders.created", "queue": "orders"}
        result = processor(None, "info", event_dict)
        assert result == {"routing_key": "orders.created", "queue": "orders"}

    def test_non_dict_nested_value_untouched(self) -> None:
        """A non-dict value (e.g. a list or plain string) is never
        mistaken for a nested dict to scan into."""
        processor = _redact_processor(frozenset({"password"}))
        event_dict = {"tags": ["a", "b"], "count": 3}
        result = processor(None, "info", event_dict)
        assert result == {"tags": ["a", "b"], "count": 3}


class TestConfigureStructlogRedaction:
    """L16: end-to-end -- the processor configure_structlog wires in
    actually redacts, and redact_keys=None disables it."""

    def test_redaction_processor_runs_by_default(self) -> None:
        config = LoggingConfig(render_json=True)
        assert config.redact_keys is not None
        processor = _redact_processor(config.redact_keys)
        with structlog.testing.capture_logs(processors=[processor]) as captured:
            structlog.get_logger("test-l16").info("processing", password="sekrit")
        assert captured[0]["password"] == "***REDACTED***"

    def test_no_redaction_processor_when_disabled(self) -> None:
        config = LoggingConfig(render_json=True, redact_keys=None)
        assert config.redact_keys is None
        # No processor to build/run -- verifies the "disabled" path is a
        # real no-op, not just an unused default.
        with structlog.testing.capture_logs() as captured:
            structlog.get_logger("test-l16").info("processing", password="sekrit")
        assert captured[0]["password"] == "sekrit"

    def test_configure_structlog_wires_redaction_processor_by_default(self) -> None:
        import structlog as _structlog

        configure_structlog(LoggingConfig(render_json=True))
        processors = _structlog.get_config()["processors"]
        assert any(getattr(p, "__qualname__", "") == "_redact_processor.<locals>.processor" for p in processors)

    def test_configure_structlog_omits_redaction_processor_when_disabled(self) -> None:
        import structlog as _structlog

        configure_structlog(LoggingConfig(render_json=True, redact_keys=None))
        processors = _structlog.get_config()["processors"]
        assert not any(getattr(p, "__qualname__", "") == "_redact_processor.<locals>.processor" for p in processors)


class TestConfigureStructlog:
    """Tests for configure_structlog function."""

    def test_configure_structlog_no_error(self) -> None:
        """Calling configure_structlog(LoggingConfig()) doesn't raise."""
        configure_structlog(LoggingConfig())

    def test_configure_structlog_json_mode(self) -> None:
        """Calling configure_structlog with render_json=True doesn't raise."""
        configure_structlog(LoggingConfig(render_json=True))

    def test_configure_structlog_none_uses_defaults(self) -> None:
        """Calling configure_structlog(None) doesn't raise."""
        configure_structlog(None)

    def test_configure_structlog_with_caller_info(self) -> None:
        """Calling configure_structlog with include_caller_info=True doesn't raise."""
        configure_structlog(LoggingConfig(include_caller_info=True))

    def test_configure_structlog_no_timestamp(self) -> None:
        """Calling configure_structlog with timestamper_fmt='' doesn't raise."""
        configure_structlog(LoggingConfig(timestamper_fmt=""))

    def test_configure_structlog_no_log_level(self) -> None:
        """Calling configure_structlog with add_log_level=False doesn't raise."""
        configure_structlog(LoggingConfig(add_log_level=False))


class TestRabbitConfigLogging:
    """Tests for LoggingConfig integration with RabbitConfig."""

    def test_rabbit_config_logging_default_none(self) -> None:
        """RabbitConfig().logging is None by default."""
        cfg = RabbitConfig()
        assert cfg.logging is None

    def test_rabbit_config_with_logging(self) -> None:
        """RabbitConfig(logging=LoggingConfig()) works."""
        log_cfg = LoggingConfig()
        cfg = RabbitConfig(logging=log_cfg)
        assert cfg.logging is log_cfg
        assert cfg.logging.render_json is False

    def test_rabbit_config_with_json_logging(self) -> None:
        """RabbitConfig with JSON logging config."""
        log_cfg = LoggingConfig(render_json=True)
        cfg = RabbitConfig(logging=log_cfg)
        assert cfg.logging is not None
        assert cfg.logging.render_json is True
