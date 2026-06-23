"""Tests for core/config.py — focused configuration objects."""

from __future__ import annotations

import pytest

from rabbitkit.core.config import (
    RETRY_DISABLED,
    BatchAckConfig,
    BatchPublishConfig,
    CompressionConfig,
    ConnectionConfig,
    ConsumerConfig,
    PoolConfig,
    PublisherConfig,
    RabbitConfig,
    RetryConfig,
    RetryDisabled,
    SecurityConfig,
    SocketConfig,
    SSLConfig,
    WorkerConfig,
)
from rabbitkit.core.types import ErrorSeverity, TopologyMode

# ── ConnectionConfig ──────────────────────────────────────────────────────


class TestConnectionConfig:
    def test_defaults(self) -> None:
        config = ConnectionConfig()
        assert config.host == "localhost"
        assert config.port == 5672
        assert config.username == "guest"
        assert config.password == "guest"
        assert config.vhost == "/"
        assert config.heartbeat == 30
        assert config.socket_timeout == 10.0
        assert config.blocked_connection_timeout == 300.0
        assert config.connection_name is None
        assert config.reconnect_backoff_base == 1.0
        assert config.reconnect_backoff_max == 30.0

    def test_url_property(self) -> None:
        config = ConnectionConfig(host="rabbit.local", port=5673, username="app", password="secret", vhost="prod")
        assert config.url == "amqp://app:secret@rabbit.local:5673/prod"

    def test_url_default_vhost(self) -> None:
        config = ConnectionConfig()
        assert config.url == "amqp://guest:guest@localhost:5672/%2F"

    def test_from_url_basic(self) -> None:
        config = ConnectionConfig.from_url("amqp://user:pass@host:5673/myvhost")
        assert config.host == "host"
        assert config.port == 5673
        assert config.username == "user"
        assert config.password == "pass"
        assert config.vhost == "myvhost"

    def test_from_url_default_vhost(self) -> None:
        config = ConnectionConfig.from_url("amqp://guest:guest@localhost:5672/%2F")
        assert config.vhost == "/"

    def test_from_url_empty_path(self) -> None:
        config = ConnectionConfig.from_url("amqp://guest:guest@localhost:5672")
        assert config.vhost == "/"

    def test_from_url_with_query_params(self) -> None:
        config = ConnectionConfig.from_url(
            "amqp://guest:guest@localhost:5672/?heartbeat=60&connection_timeout=5"
        )
        assert config.heartbeat == 60
        assert config.socket_timeout == 5.0

    def test_from_url_defaults(self) -> None:
        config = ConnectionConfig.from_url("amqp://localhost")
        assert config.host == "localhost"
        assert config.port == 5672
        assert config.username == "guest"
        assert config.password == "guest"

    def test_frozen(self) -> None:
        config = ConnectionConfig()
        with pytest.raises(AttributeError):
            config.host = "other"  # type: ignore[misc]


# ── SocketConfig ──────────────────────────────────────────────────────────


class TestSocketConfig:
    def test_defaults(self) -> None:
        config = SocketConfig()
        assert config.tcp_nodelay is True
        assert config.tcp_keepidle == 10
        assert config.tcp_keepintvl == 5
        assert config.tcp_keepcnt == 3
        assert config.tcp_sndbuf == 196608
        assert config.tcp_rcvbuf == 196608

    def test_frozen(self) -> None:
        config = SocketConfig()
        with pytest.raises(AttributeError):
            config.tcp_nodelay = False  # type: ignore[misc]


# ── SSLConfig ─────────────────────────────────────────────────────────────


class TestSSLConfig:
    def test_defaults(self) -> None:
        config = SSLConfig()
        assert config.enabled is False
        assert config.certfile is None
        assert config.keyfile is None
        assert config.ca_certs is None
        assert config.cert_reqs == "CERT_REQUIRED"
        assert config.server_hostname is None

    def test_enabled(self) -> None:
        config = SSLConfig(
            enabled=True,
            certfile="/path/to/cert.pem",
            keyfile="/path/to/key.pem",
            ca_certs="/path/to/ca.pem",
        )
        assert config.enabled is True
        assert config.certfile == "/path/to/cert.pem"


# ── SecurityConfig ────────────────────────────────────────────────────────


class TestSecurityConfig:
    def test_defaults(self) -> None:
        config = SecurityConfig()
        assert config.mechanism == "PLAIN"
        assert config.ssl.enabled is False

    def test_with_ssl(self) -> None:
        config = SecurityConfig(ssl=SSLConfig(enabled=True))
        assert config.ssl.enabled is True


# ── PublisherConfig ───────────────────────────────────────────────────────


class TestPublisherConfig:
    def test_defaults(self) -> None:
        config = PublisherConfig()
        assert config.exchange == ""
        assert config.confirm_delivery is True
        assert config.confirm_timeout == 5.0
        assert config.mandatory is False
        assert config.persistent is True


# ── ConsumerConfig ────────────────────────────────────────────────────────


class TestConsumerConfig:
    def test_defaults(self) -> None:
        config = ConsumerConfig()
        assert config.prefetch_count == 10
        assert config.graceful_timeout == 30.0


# ── PoolConfig ────────────────────────────────────────────────────────────


class TestPoolConfig:
    def test_defaults(self) -> None:
        config = PoolConfig()
        assert config.channel_pool_size == 10
        assert config.publisher_connections == 1
        assert config.consumer_connections == 1


# ── RetryConfig ───────────────────────────────────────────────────────────


class TestRetryConfig:
    def test_defaults(self) -> None:
        config = RetryConfig()
        assert config.max_retries == 4
        assert config.delays == (5, 30, 120, 600)
        assert config.retry_header == "x-rabbitkit-retry-count"
        assert config.jitter_factor == 0.1
        assert config.dead_letter_exchange == ""
        assert config.per_queue is True
        assert config.unknown_policy == ErrorSeverity.PERMANENT

    def test_custom(self) -> None:
        config = RetryConfig(max_retries=2, delays=(10, 60), jitter_factor=0.2)
        assert config.max_retries == 2
        assert config.delays == (10, 60)
        assert config.jitter_factor == 0.2


# ── CompressionConfig ────────────────────────────────────────────────────


class TestCompressionConfig:
    def test_defaults(self) -> None:
        config = CompressionConfig()
        assert config.algorithm == "gzip"
        assert config.threshold == 1024
        assert config.level == 6


# ── RetryDisabled ─────────────────────────────────────────────────────────


class TestRetryDisabled:
    def test_is_singleton(self) -> None:
        a = RetryDisabled()
        b = RetryDisabled()
        assert a is b
        assert a is RETRY_DISABLED

    def test_repr(self) -> None:
        assert repr(RETRY_DISABLED) == "RETRY_DISABLED"

    def test_falsy(self) -> None:
        assert not RETRY_DISABLED
        assert bool(RETRY_DISABLED) is False

    def test_is_not_none(self) -> None:
        assert RETRY_DISABLED is not None


# ── Placeholder configs ──────────────────────────────────────────────────


class TestPlaceholderConfigs:
    def test_batch_publish_defaults(self) -> None:
        config = BatchPublishConfig()
        assert config.batch_size == 100
        assert config.flush_interval_ms == 50
        assert config.max_in_flight == 1000

    def test_batch_ack_defaults(self) -> None:
        config = BatchAckConfig()
        assert config.batch_size == 100
        assert config.flush_interval_ms == 200

    def test_worker_config_defaults(self) -> None:
        config = WorkerConfig()
        assert config.worker_count == 1
        assert config.prefetch_per_worker is None


# ── RabbitConfig (composition) ────────────────────────────────────────────


class TestRabbitConfig:
    def test_defaults(self) -> None:
        config = RabbitConfig()
        assert config.connection.host == "localhost"
        assert config.socket.tcp_nodelay is True
        assert config.security.mechanism == "PLAIN"
        assert config.publisher.confirm_delivery is True
        assert config.consumer.prefetch_count == 10
        assert config.pool.channel_pool_size == 10
        assert config.topology_mode == TopologyMode.AUTO_DECLARE
        assert config.retry is None
        assert config.compression is None

    def test_with_retry(self) -> None:
        config = RabbitConfig(retry=RetryConfig(max_retries=2))
        assert config.retry is not None
        assert config.retry.max_retries == 2

    def test_with_compression(self) -> None:
        config = RabbitConfig(compression=CompressionConfig(algorithm="zstd"))
        assert config.compression is not None
        assert config.compression.algorithm == "zstd"

    def test_full_composition(self) -> None:
        config = RabbitConfig(
            connection=ConnectionConfig(host="rabbit.prod"),
            socket=SocketConfig(tcp_nodelay=False),
            security=SecurityConfig(ssl=SSLConfig(enabled=True)),
            publisher=PublisherConfig(exchange="events"),
            consumer=ConsumerConfig(prefetch_count=50),
            pool=PoolConfig(channel_pool_size=20),
            topology_mode=TopologyMode.PASSIVE_ONLY,
            retry=RetryConfig(),
            compression=CompressionConfig(),
        )
        assert config.connection.host == "rabbit.prod"
        assert config.socket.tcp_nodelay is False
        assert config.security.ssl.enabled is True
        assert config.publisher.exchange == "events"
        assert config.consumer.prefetch_count == 50
        assert config.pool.channel_pool_size == 20
        assert config.topology_mode == TopologyMode.PASSIVE_ONLY


class TestConnectionConfigFromUrlBlockedTimeout:
    def test_from_url_with_blocked_connection_timeout(self) -> None:
        """Line 69: blocked_connection_timeout parsed from query string."""
        from rabbitkit.core.config import ConnectionConfig

        cfg = ConnectionConfig.from_url(
            "amqp://user:pass@localhost//?blocked_connection_timeout=45"
        )
        assert cfg.blocked_connection_timeout == 45.0
