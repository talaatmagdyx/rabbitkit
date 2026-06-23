"""Configuration: RabbitSettings — load config from RABBITMQ_* env vars.

Requires: pip install "rabbitkit[settings]"

Reads configuration from environment variables or a .env file.
Useful for 12-factor apps and containerized deployments.

Run:
    RABBITMQ_HOST=localhost RABBITMQ_USER=guest python examples/config/02_env_config.py

Requirements:
    pip install "rabbitkit[async,settings]"
    RabbitMQ running on localhost:5672
"""

import os

# ── Check availability ────────────────────────────────────────────────────────
from rabbitkit.core.env_config import _PYDANTIC_SETTINGS_AVAILABLE

if not _PYDANTIC_SETTINGS_AVAILABLE:
    print("pydantic-settings not installed. Run: pip install 'rabbitkit[settings]'")
    exit(1)

from rabbitkit.core.env_config import RabbitSettings
from rabbitkit.async_ import AsyncBroker


# ── 1. Basic usage — reads from environment ───────────────────────────────────
print("=== Reading from environment ===")

# Set env vars programmatically for this demo
os.environ.setdefault("RABBITMQ_HOST", "localhost")
os.environ.setdefault("RABBITMQ_USER", "guest")
os.environ.setdefault("RABBITMQ_PASSWORD", "guest")
os.environ.setdefault("RABBITMQ_VHOST", "/")
os.environ.setdefault("RABBITMQ_PREFETCH_COUNT", "10")
os.environ.setdefault("RABBITMQ_TOPOLOGY_MODE", "AUTO_DECLARE")

settings = RabbitSettings()  # reads RABBITMQ_* vars automatically
config = settings.to_rabbit_config()

print(f"Host:          {config.connection.host}")
print(f"Port:          {config.connection.port}")
print(f"User:          {config.connection.username}")
print(f"VHost:         {config.connection.vhost!r}")
print(f"Prefetch:      {config.consumer.prefetch_count}")
print(f"TopologyMode:  {config.topology_mode.name}")


# ── 2. Override at runtime ────────────────────────────────────────────────────
print("\n=== Runtime overrides ===")

settings_staging = RabbitSettings(
    host="rabbitmq.staging.internal",
    prefetch_count=5,
    confirm_delivery=False,
)
staging_config = settings_staging.to_rabbit_config()
print(f"Staging host: {staging_config.connection.host}")
print(f"Prefetch:     {staging_config.consumer.prefetch_count}")


# ── 3. .env file support ──────────────────────────────────────────────────────
# pydantic-settings automatically reads .env files.
# Create a .env file:
#
#   # .env
#   RABBITMQ_HOST=rabbitmq.prod.internal
#   RABBITMQ_USER=myapp
#   RABBITMQ_PASSWORD=from-vault
#   RABBITMQ_VHOST=/production
#   RABBITMQ_PREFETCH_COUNT=20
#   RABBITMQ_CHANNEL_POOL_SIZE=20
#   RABBITMQ_TOPOLOGY_MODE=AUTO_DECLARE
#
# Then just instantiate:
#   settings = RabbitSettings()   # picks up .env automatically


# ── 4. Use in broker ─────────────────────────────────────────────────────────
print("\n=== Broker with env config ===")

broker = AsyncBroker(config)
print(f"Broker created: {broker.config.connection.host}:{broker.config.connection.port}")


# ── 5. All supported env vars ────────────────────────────────────────────────
print("\n=== Supported RABBITMQ_* variables ===")
var_table = [
    ("RABBITMQ_HOST",                      "localhost",      "str"),
    ("RABBITMQ_PORT",                      "5672",           "int"),
    ("RABBITMQ_USER",                      "guest",          "str"),
    ("RABBITMQ_PASSWORD",                  "guest",          "str (sensitive!)"),
    ("RABBITMQ_VHOST",                     "/",              "str"),
    ("RABBITMQ_HEARTBEAT",                 "30",             "int (seconds)"),
    ("RABBITMQ_SOCKET_TIMEOUT",            "10.0",           "float (seconds)"),
    ("RABBITMQ_BLOCKED_CONNECTION_TIMEOUT","300.0",          "float (seconds)"),
    ("RABBITMQ_CONNECTION_NAME",           "None",           "str | None"),
    ("RABBITMQ_PREFETCH_COUNT",            "10",             "int"),
    ("RABBITMQ_CONFIRM_DELIVERY",          "true",           "bool"),
    ("RABBITMQ_CHANNEL_POOL_SIZE",         "10",             "int"),
    ("RABBITMQ_TOPOLOGY_MODE",             "AUTO_DECLARE",   "AUTO_DECLARE|PASSIVE_ONLY|MANUAL"),
]
for name, default, type_ in var_table:
    print(f"  {name:<45} default={default!r:<15} type={type_}")
