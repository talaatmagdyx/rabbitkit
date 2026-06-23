"""Tests for structured logging configuration."""

from __future__ import annotations

import dataclasses

import pytest

from rabbitkit.core.config import RabbitConfig
from rabbitkit.core.logging import LoggingConfig, configure_structlog


class TestLoggingConfig:
    """Tests for LoggingConfig dataclass."""

    def test_logging_config_defaults(self) -> None:
        """LoggingConfig() has correct defaults."""
        cfg = LoggingConfig()
        assert cfg.render_json is False
        assert cfg.add_log_level is True
        assert cfg.timestamper_fmt == "iso"
        assert cfg.include_caller_info is False

    def test_logging_config_frozen(self) -> None:
        """Cannot modify LoggingConfig after creation."""
        cfg = LoggingConfig()
        with pytest.raises(dataclasses.FrozenInstanceError):
            cfg.render_json = True  # type: ignore[misc]


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
