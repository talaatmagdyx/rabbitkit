"""Environment-variable driven RabbitMQ configuration.

Requires: ``pip install rabbitkit[settings]``

Maps ``RABBITMQ_*`` environment variables to ``RabbitConfig`` using
pydantic-settings ``BaseSettings``.  Supports ``.env`` files, environment
overrides, and type coercion out of the box.

Lazy import — no ``ImportError`` at import time when pydantic-settings is
absent; a placeholder class is installed instead and raises on instantiation.

Quick start
-----------
    # .env (or set as real environment variables)
    RABBITMQ_HOST=rabbitmq.prod.internal
    RABBITMQ_PORT=5672
    RABBITMQ_USER=myapp
    RABBITMQ_PASSWORD=secret
    RABBITMQ_VHOST=/production
    RABBITMQ_PREFETCH_COUNT=20
    RABBITMQ_TOPOLOGY_MODE=AUTO_DECLARE

    # application code
    from rabbitkit.core.env_config import RabbitSettings
    from rabbitkit.async_ import AsyncBroker

    settings = RabbitSettings()           # reads from env / .env automatically
    config   = settings.to_rabbit_config()
    broker   = AsyncBroker(config)

Supported environment variables
--------------------------------
All variables are prefixed with ``RABBITMQ_`` and case-insensitive:

Connection
    RABBITMQ_HOST                        (str,   default "localhost")
    RABBITMQ_PORT                        (int,   default 5672)
    RABBITMQ_USER                        (str,   default "guest")
    RABBITMQ_PASSWORD                    (str,   default "guest")
    RABBITMQ_VHOST                       (str,   default "/")
    RABBITMQ_HEARTBEAT                   (int,   default 30)
    RABBITMQ_SOCKET_TIMEOUT              (float, default 10.0)
    RABBITMQ_BLOCKED_CONNECTION_TIMEOUT  (float, default 60.0)
    RABBITMQ_CONNECTION_NAME             (str | None, default None)
    RABBITMQ_RECONNECT_BACKOFF_BASE      (float, default 1.0)
    RABBITMQ_RECONNECT_BACKOFF_MAX       (float, default 30.0)

Consumer
    RABBITMQ_PREFETCH_COUNT              (int,   default 10)
    RABBITMQ_GRACEFUL_TIMEOUT            (float, default 30.0)

Publisher
    RABBITMQ_CONFIRM_DELIVERY            (bool,  default True)
    RABBITMQ_CONFIRM_TIMEOUT             (float, default 5.0)
    RABBITMQ_MANDATORY                   (bool,  default False)
    RABBITMQ_PERSISTENT                  (bool,  default True)
    RABBITMQ_DEFAULT_EXCHANGE            (str,   default "")

Pool
    RABBITMQ_CHANNEL_POOL_SIZE           (int,   default 10)
    RABBITMQ_PUBLISHER_CONNECTIONS       (int,   default 1)
    RABBITMQ_CONSUMER_CONNECTIONS        (int,   default 1)
    RABBITMQ_CHANNEL_ACQUIRE_TIMEOUT     (float, default 10.0)

SSL
    RABBITMQ_SSL_ENABLED                 (bool,  default False)
    RABBITMQ_SSL_CERTFILE                (str | None, default None)
    RABBITMQ_SSL_KEYFILE                 (str | None, default None)
    RABBITMQ_SSL_CA_CERTS                (str | None, default None)
    RABBITMQ_SSL_SERVER_HOSTNAME         (str | None, default None)

Retry  (set RABBITMQ_RETRY_MAX_RETRIES > 0 to enable)
    RABBITMQ_RETRY_MAX_RETRIES           (int,   default 0 — disabled)
    RABBITMQ_RETRY_DELAYS                (str,   default "5,30,120,600" — comma-separated ints)
    RABBITMQ_RETRY_JITTER_FACTOR         (float, default 0.1)

Topology
    RABBITMQ_TOPOLOGY_MODE               (str,   default "AUTO_DECLARE")
                                         valid: AUTO_DECLARE, PASSIVE_ONLY, MANUAL

Override at runtime
-------------------
pydantic-settings respects constructor keyword arguments, so you can mix
env-file defaults with runtime overrides:

    settings = RabbitSettings(host="staging-rabbit", prefetch_count=5)
    config   = settings.to_rabbit_config()

Checking availability
---------------------
    from rabbitkit.core.env_config import _PYDANTIC_SETTINGS_AVAILABLE

    if _PYDANTIC_SETTINGS_AVAILABLE:
        settings = RabbitSettings()
    else:
        config = RabbitConfig(connection=ConnectionConfig(...))
"""

from __future__ import annotations

from typing import Any

_PYDANTIC_SETTINGS_AVAILABLE = False

try:
    from pydantic import Field, SecretStr
    from pydantic_settings import BaseSettings, SettingsConfigDict

    _PYDANTIC_SETTINGS_AVAILABLE = True
except ImportError:  # pragma: no cover
    pass  # pragma: no cover

if _PYDANTIC_SETTINGS_AVAILABLE:
    from rabbitkit.core.config import (
        ConnectionConfig,
        ConsumerConfig,
        PoolConfig,
        PublisherConfig,
        RabbitConfig,
        RetryConfig,
        SecurityConfig,
        SSLConfig,
    )
    from rabbitkit.core.types import TopologyMode

    class RabbitSettings(BaseSettings):
        """Load RabbitMQ config from RABBITMQ_* environment variables.

        Example::

            settings = RabbitSettings()  # reads from env / .env
            config = settings.to_rabbit_config()
            broker = AsyncBroker(config)
        """

        model_config = SettingsConfigDict(
            env_prefix="RABBITMQ_",
            case_sensitive=False,
        )

        # Connection
        host: str = "localhost"
        port: int = 5672
        user: str = Field(default="guest")
        password: SecretStr = SecretStr("guest")
        vhost: str = "/"
        heartbeat: int = 30
        socket_timeout: float = 10.0
        blocked_connection_timeout: float = 60.0
        connection_name: str | None = None
        reconnect_backoff_base: float = 1.0
        reconnect_backoff_max: float = 30.0

        # Consumer
        prefetch_count: int = 10
        graceful_timeout: float = 30.0

        # Publisher
        confirm_delivery: bool = True
        confirm_timeout: float = 5.0
        mandatory: bool = False
        persistent: bool = True
        default_exchange: str = ""

        # Pool
        channel_pool_size: int = 10
        publisher_connections: int = 1
        consumer_connections: int = 1
        channel_acquire_timeout: float = 10.0

        # SSL
        ssl_enabled: bool = False
        ssl_certfile: str | None = None
        ssl_keyfile: str | None = None
        ssl_ca_certs: str | None = None
        ssl_server_hostname: str | None = None

        # Retry (0 = disabled, no RetryConfig created)
        retry_max_retries: int = 0
        retry_delays: str = "5,30,120,600"
        retry_jitter_factor: float = 0.1

        # Topology
        topology_mode: str = "AUTO_DECLARE"

        def to_rabbit_config(self) -> RabbitConfig:
            """Convert to a RabbitConfig dataclass."""
            ssl = SSLConfig(
                enabled=self.ssl_enabled,
                certfile=self.ssl_certfile,
                keyfile=self.ssl_keyfile,
                ca_certs=self.ssl_ca_certs,
                server_hostname=self.ssl_server_hostname,
            )
            retry: RetryConfig | None = None
            if self.retry_max_retries > 0:
                delays = tuple(int(d) for d in self.retry_delays.split(",") if d.strip())
                retry = RetryConfig(
                    max_retries=self.retry_max_retries,
                    delays=delays,
                    jitter_factor=self.retry_jitter_factor,
                )
            return RabbitConfig(
                connection=ConnectionConfig(
                    host=self.host,
                    port=self.port,
                    username=self.user,
                    password=self.password.get_secret_value(),
                    vhost=self.vhost,
                    heartbeat=self.heartbeat,
                    socket_timeout=self.socket_timeout,
                    blocked_connection_timeout=self.blocked_connection_timeout,
                    connection_name=self.connection_name,
                    reconnect_backoff_base=self.reconnect_backoff_base,
                    reconnect_backoff_max=self.reconnect_backoff_max,
                ),
                security=SecurityConfig(ssl=ssl),
                consumer=ConsumerConfig(
                    prefetch_count=self.prefetch_count,
                    graceful_timeout=self.graceful_timeout,
                ),
                publisher=PublisherConfig(
                    confirm_delivery=self.confirm_delivery,
                    confirm_timeout=self.confirm_timeout,
                    mandatory=self.mandatory,
                    persistent=self.persistent,
                    exchange=self.default_exchange,
                ),
                pool=PoolConfig(
                    channel_pool_size=self.channel_pool_size,
                    publisher_connections=self.publisher_connections,
                    consumer_connections=self.consumer_connections,
                    channel_acquire_timeout=self.channel_acquire_timeout,
                ),
                topology_mode=TopologyMode[self.topology_mode.upper()],
                retry=retry,
            )

else:  # pragma: no cover

    class RabbitSettings:  # type: ignore[no-redef]  # pragma: no cover
        """Placeholder — pydantic-settings not installed."""

        def __init__(self, **_: Any) -> None:
            raise ImportError(
                "RabbitSettings requires pydantic-settings. Install with: pip install rabbitkit[settings]"
            )

        def to_rabbit_config(self) -> Any:
            """Not available without pydantic-settings."""
            raise ImportError("pydantic-settings not installed")
