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
    MetricsConfig,
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
        assert config.blocked_connection_timeout == 60.0
        assert config.connection_name is None
        assert config.reconnect_backoff_base == 1.0
        assert config.reconnect_backoff_max == 30.0

    def test_url_property(self) -> None:
        config = ConnectionConfig(host="rabbit.local", port=5673, username="app", password="secret", vhost="prod")
        assert config.url == "amqp://app:secret@rabbit.local:5673/prod"

    def test_url_default_vhost(self) -> None:
        config = ConnectionConfig()
        assert config.url == "amqp://guest:guest@localhost:5672/%2F"

    def test_url_encodes_special_credentials(self) -> None:
        """A password/username/vhost containing reserved chars does not corrupt host/port parse."""
        from urllib.parse import urlparse

        pwd = "p@ss:wo/rd#1"
        user = "u:ser"
        vhost = "vh/with#special"
        config = ConnectionConfig(host="rabbit.local", port=5673, username=user, password=pwd, vhost=vhost)
        url = config.url
        # Round-trip: the encoded URL parses back to the original fields.
        parsed = urlparse(url)
        assert parsed.hostname == "rabbit.local"
        assert parsed.port == 5673
        from urllib.parse import unquote

        assert unquote(parsed.username) == user
        assert unquote(parsed.password) == pwd
        # vhost path (after port) round-trips too.
        assert unquote(parsed.path.lstrip("/")) == vhost

    def test_guest_credentials_warn_for_non_local_host(self, recwarn: pytest.WarningsRecorder) -> None:
        """A guest/guest config against a non-local host emits one UserWarning."""
        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            ConnectionConfig(host="rabbit.prod")
        guest_warnings = [w for w in caught if issubclass(w.category, UserWarning)]
        assert len(guest_warnings) == 1
        assert "guest" in str(guest_warnings[0].message)

    def test_guest_credentials_no_warn_for_localhost(self, recwarn: pytest.WarningsRecorder) -> None:
        """guest/guest against localhost/127.0.0.1/::1 does NOT warn."""
        import warnings

        for host in ("localhost", "127.0.0.1", "::1"):
            with warnings.catch_warnings(record=True) as caught:
                warnings.simplefilter("always")
                ConnectionConfig(host=host)
            assert not [w for w in caught if issubclass(w.category, UserWarning)], (
                f"unexpected guest warning for host={host!r}"
            )

    def test_non_guest_credentials_no_warn(self, recwarn: pytest.WarningsRecorder) -> None:
        """Non-guest credentials against a non-local host do NOT warn."""
        import warnings

        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always")
            ConnectionConfig(host="rabbit.prod", username="app", password="secret")
        assert not [w for w in caught if issubclass(w.category, UserWarning)]

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
        config = ConnectionConfig.from_url("amqp://guest:guest@localhost:5672/?heartbeat=60&connection_timeout=5")
        assert config.heartbeat == 60
        assert config.socket_timeout == 5.0

    def test_from_url_unquotes_encoded_credentials(self) -> None:
        """Percent-encoded credentials in an AMQP URL are decoded, and the
        ``url`` property re-encodes them (round-trip without double-encoding)."""
        config = ConnectionConfig.from_url("amqp://user%40:p%40ss@host/")
        assert config.username == "user@"
        assert config.password == "p@ss"
        assert config.host == "host"
        # The url property re-encodes the reserved characters.
        assert config.url == "amqp://user%40:p%40ss@host:5672/%2F"

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

    def test_empty_delays_strict_raises(self) -> None:
        """Empty delays with max_retries>0 + strict_delays=True raises (I-Low).

        Previously the guard was ``if self.delays and ...`` which short-circuited
        on an empty tuple, letting the misconfiguration through to IndexError at
        runtime. The guard now catches it at construction time.
        """
        with pytest.raises(ValueError, match="has 0 entries but max_retries"):
            RetryConfig(max_retries=4, delays=(), strict_delays=True)

    def test_empty_delays_non_strict_warns(self) -> None:
        """Empty delays with max_retries>0 + strict_delays=False warns (I-Low)."""
        with pytest.warns(UserWarning, match="has 0 entries but max_retries"):
            RetryConfig(max_retries=4, delays=(), strict_delays=False)

    def test_empty_delays_zero_max_retries_ok(self) -> None:
        "max_retries=0 with empty delays is valid (no retry attempts)."
        config = RetryConfig(max_retries=0, delays=(), strict_delays=True)
        assert config.max_retries == 0
        assert config.delays == ()


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

        cfg = ConnectionConfig.from_url("amqp://user:pass@localhost//?blocked_connection_timeout=45")
        assert cfg.blocked_connection_timeout == 45.0


class TestBatchPublishConfigValidation:
    def test_batch_size_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="batch_size must be > 0"):
            BatchPublishConfig(batch_size=0)

    def test_batch_size_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="batch_size must be > 0"):
            BatchPublishConfig(batch_size=-1)

    def test_flush_interval_ms_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="flush_interval_ms must be >= 0"):
            BatchPublishConfig(flush_interval_ms=-1)

    def test_flush_interval_ms_zero_is_valid(self) -> None:
        cfg = BatchPublishConfig(flush_interval_ms=0)
        assert cfg.flush_interval_ms == 0

    def test_max_in_flight_zero_raises(self) -> None:
        with pytest.raises(ValueError, match="max_in_flight must be > 0"):
            BatchPublishConfig(max_in_flight=0)

    def test_max_in_flight_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="max_in_flight must be > 0"):
            BatchPublishConfig(max_in_flight=-1)

    def test_flush_workers_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="flush_workers must be >= 0"):
            BatchPublishConfig(flush_workers=-1)

    def test_flush_workers_zero_is_valid(self) -> None:
        cfg = BatchPublishConfig(flush_workers=0)
        assert cfg.flush_workers == 0

    def test_flush_workers_positive_is_valid(self) -> None:
        cfg = BatchPublishConfig(flush_workers=5)
        assert cfg.flush_workers == 5


class TestMetricsConfigProperties:
    def test_handler_duration_seconds_default(self) -> None:
        cfg = MetricsConfig()
        assert cfg.handler_duration_seconds == "rabbitkit_handler_duration_seconds"

    def test_handler_errors_total(self) -> None:
        cfg = MetricsConfig()
        assert cfg.handler_errors_total == "rabbitkit_handler_errors_total"

    def test_messages_acked_total(self) -> None:
        cfg = MetricsConfig()
        assert cfg.messages_acked_total == "rabbitkit_messages_acked_total"

    def test_messages_nacked_total(self) -> None:
        cfg = MetricsConfig()
        assert cfg.messages_nacked_total == "rabbitkit_messages_nacked_total"

    def test_messages_rejected_total(self) -> None:
        cfg = MetricsConfig()
        assert cfg.messages_rejected_total == "rabbitkit_messages_rejected_total"

    def test_messages_retried_total(self) -> None:
        cfg = MetricsConfig()
        assert cfg.messages_retried_total == "rabbitkit_messages_retried_total"

    def test_messages_dead_lettered_total(self) -> None:
        cfg = MetricsConfig()
        assert cfg.messages_dead_lettered_total == "rabbitkit_messages_dead_lettered_total"

    def test_publish_total_default(self) -> None:
        cfg = MetricsConfig()
        assert cfg.publish_total == "rabbitkit_publish_total"

    def test_publish_total_custom_counter(self) -> None:
        cfg = MetricsConfig(published_counter="my_pub")
        assert cfg.publish_total == "my_pub"

    def test_publish_failures_total(self) -> None:
        cfg = MetricsConfig()
        assert cfg.publish_failures_total == "rabbitkit_publish_failures_total"

    def test_publish_confirm_latency_seconds(self) -> None:
        cfg = MetricsConfig()
        assert cfg.publish_confirm_latency_seconds == "rabbitkit_publish_confirm_latency_seconds"

    def test_in_flight_messages(self) -> None:
        cfg = MetricsConfig()
        assert cfg.in_flight_messages == "rabbitkit_in_flight_messages"

    def test_worker_pool_pending(self) -> None:
        cfg = MetricsConfig()
        assert cfg.worker_pool_pending == "rabbitkit_worker_pool_pending"

    def test_broker_connected(self) -> None:
        cfg = MetricsConfig()
        assert cfg.broker_connected == "rabbitkit_broker_connected"

    def test_consumer_active(self) -> None:
        cfg = MetricsConfig()
        assert cfg.consumer_active == "rabbitkit_consumer_active"
