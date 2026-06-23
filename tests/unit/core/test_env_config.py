"""Tests for pydantic-settings config (F3)."""

from __future__ import annotations

import pytest

from rabbitkit.core.env_config import _PYDANTIC_SETTINGS_AVAILABLE, RabbitSettings


@pytest.mark.skipif(not _PYDANTIC_SETTINGS_AVAILABLE, reason="pydantic-settings not installed")
class TestRabbitSettings:

    def test_defaults(self) -> None:
        settings = RabbitSettings()
        assert settings.host == "localhost"
        assert settings.port == 5672
        assert settings.user == "guest"
        assert settings.password == "guest"
        assert settings.vhost == "/"

    def test_to_rabbit_config(self) -> None:
        settings = RabbitSettings()
        config = settings.to_rabbit_config()
        assert config.connection.host == "localhost"
        assert config.connection.port == 5672
        assert config.consumer.prefetch_count == 10
        assert config.publisher.confirm_delivery is True

    def test_env_override(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RABBITMQ_HOST", "prod.rabbit.local")
        monkeypatch.setenv("RABBITMQ_PORT", "5673")
        monkeypatch.setenv("RABBITMQ_USER", "myapp")
        settings = RabbitSettings()
        assert settings.host == "prod.rabbit.local"
        assert settings.port == 5673
        assert settings.user == "myapp"

    def test_env_to_config(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RABBITMQ_HOST", "prod.rabbit.local")
        monkeypatch.setenv("RABBITMQ_PREFETCH_COUNT", "50")
        settings = RabbitSettings()
        config = settings.to_rabbit_config()
        assert config.connection.host == "prod.rabbit.local"
        assert config.consumer.prefetch_count == 50

    def test_topology_mode_parsing(self) -> None:
        settings = RabbitSettings(topology_mode="PASSIVE_ONLY")
        config = settings.to_rabbit_config()
        from rabbitkit.core.types import TopologyMode
        assert config.topology_mode == TopologyMode.PASSIVE_ONLY

    def test_confirm_delivery_false(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RABBITMQ_CONFIRM_DELIVERY", "false")
        settings = RabbitSettings()
        config = settings.to_rabbit_config()
        assert config.publisher.confirm_delivery is False


@pytest.mark.skipif(not _PYDANTIC_SETTINGS_AVAILABLE, reason="pydantic-settings not installed")
class TestRabbitSettingsExpanded:

    def test_reconnect_backoff_fields(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RABBITMQ_RECONNECT_BACKOFF_BASE", "2.5")
        monkeypatch.setenv("RABBITMQ_RECONNECT_BACKOFF_MAX", "60.0")
        config = RabbitSettings().to_rabbit_config()
        assert config.connection.reconnect_backoff_base == 2.5
        assert config.connection.reconnect_backoff_max == 60.0

    def test_graceful_timeout(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RABBITMQ_GRACEFUL_TIMEOUT", "45.0")
        config = RabbitSettings().to_rabbit_config()
        assert config.consumer.graceful_timeout == 45.0

    def test_publisher_extras(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RABBITMQ_CONFIRM_TIMEOUT", "3.0")
        monkeypatch.setenv("RABBITMQ_MANDATORY", "true")
        monkeypatch.setenv("RABBITMQ_PERSISTENT", "false")
        monkeypatch.setenv("RABBITMQ_DEFAULT_EXCHANGE", "my-exchange")
        config = RabbitSettings().to_rabbit_config()
        assert config.publisher.confirm_timeout == 3.0
        assert config.publisher.mandatory is True
        assert config.publisher.persistent is False
        assert config.publisher.exchange == "my-exchange"

    def test_pool_extras(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RABBITMQ_PUBLISHER_CONNECTIONS", "2")
        monkeypatch.setenv("RABBITMQ_CONSUMER_CONNECTIONS", "3")
        monkeypatch.setenv("RABBITMQ_CHANNEL_ACQUIRE_TIMEOUT", "15.0")
        config = RabbitSettings().to_rabbit_config()
        assert config.pool.publisher_connections == 2
        assert config.pool.consumer_connections == 3
        assert config.pool.channel_acquire_timeout == 15.0

    def test_ssl_fields_assembled(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RABBITMQ_SSL_ENABLED", "true")
        monkeypatch.setenv("RABBITMQ_SSL_CERTFILE", "/certs/client.crt")
        monkeypatch.setenv("RABBITMQ_SSL_KEYFILE", "/certs/client.key")
        monkeypatch.setenv("RABBITMQ_SSL_CA_CERTS", "/certs/ca.crt")
        monkeypatch.setenv("RABBITMQ_SSL_SERVER_HOSTNAME", "rabbit.internal")
        config = RabbitSettings().to_rabbit_config()
        ssl = config.security.ssl
        assert ssl.enabled is True
        assert ssl.certfile == "/certs/client.crt"
        assert ssl.keyfile == "/certs/client.key"
        assert ssl.ca_certs == "/certs/ca.crt"
        assert ssl.server_hostname == "rabbit.internal"

    def test_ssl_disabled_by_default(self) -> None:
        config = RabbitSettings().to_rabbit_config()
        assert config.security.ssl.enabled is False

    def test_retry_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RABBITMQ_RETRY_MAX_RETRIES", "3")
        monkeypatch.setenv("RABBITMQ_RETRY_DELAYS", "5,30,120")
        monkeypatch.setenv("RABBITMQ_RETRY_JITTER_FACTOR", "0.2")
        config = RabbitSettings().to_rabbit_config()
        assert config.retry is not None
        assert config.retry.max_retries == 3
        assert config.retry.delays == (5, 30, 120)
        assert config.retry.jitter_factor == 0.2

    def test_no_retry_when_zero(self) -> None:
        config = RabbitSettings().to_rabbit_config()
        assert config.retry is None

    def test_retry_disabled_explicit_zero(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RABBITMQ_RETRY_MAX_RETRIES", "0")
        config = RabbitSettings().to_rabbit_config()
        assert config.retry is None
