"""TLS against a real broker, both transports.

The async transport built an ``amqp://`` URL even with ``SSLConfig(enabled=True)``.
aiormq picks TLS by the URL scheme only, so the connection was plaintext: it
failed against the TLS port and, against a plain port, connected UNENCRYPTED
with no error. Nothing caught it because no test ever spoke real TLS.

The broker generates a throwaway CA and server certificate at startup, so no
key material is committed (and GitGuardian has nothing to flag).
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from tests.integration.conftest import skip_without_docker

pytestmark = pytest.mark.integration

_BOOT = (
    "set -e; mkdir -p /tmp/tls && cd /tmp/tls"
    " && openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.pem -days 2 -subj /CN=rk-test-ca"
    " && openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj /CN=localhost"
    " && printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\\n' > ext.cnf"
    " && openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial -out server.pem"
    " -days 2 -extfile ext.cnf"
    " && chmod 644 /tmp/tls/*"
    " && printf 'listeners.tcp.default = 5672\\nlisteners.ssl.default = 5671\\n"
    "ssl_options.cacertfile = /tmp/tls/ca.pem\\nssl_options.certfile = /tmp/tls/server.pem\\n"
    "ssl_options.keyfile = /tmp/tls/server.key\\nssl_options.verify = verify_none\\n"
    "ssl_options.fail_if_no_peer_cert = false\\nloopback_users = none\\n' > /tmp/tls/rabbitmq.conf"
    " && exec docker-entrypoint.sh rabbitmq-server"
)


def _exec(container: Any, cmd: list[str]) -> str:
    result = container.get_wrapped_container().exec_run(cmd)
    output = result[1] if isinstance(result, tuple) else result.output
    return output.decode("utf-8", "replace") if isinstance(output, bytes) else str(output)


@pytest.fixture(scope="module")
def tls_rabbit(tmp_path_factory: pytest.TempPathFactory) -> Iterator[dict[str, Any]]:
    skip_without_docker()
    from testcontainers.core.container import DockerContainer  # type: ignore[import-untyped]

    container = (
        DockerContainer("rabbitmq:3.13-alpine")
        .with_env("RABBITMQ_CONFIG_FILE", "/tmp/tls/rabbitmq.conf")  # noqa: S108 — a path inside the container
        .with_exposed_ports(5671, 5672)
        .with_command(["sh", "-c", _BOOT])
    )
    with container:
        # Wait on the broker's own log: an exec before the container is
        # running is a 409 from the Docker API.
        wrapped = container.get_wrapped_container()
        deadline = time.monotonic() + 90
        while True:
            logs = wrapped.logs()
            if b"started TLS (SSL) listener" in logs and b"Server startup complete" in logs:
                break
            assert time.monotonic() < deadline, "TLS listener never came up"
            time.sleep(0.5)
        ca = tmp_path_factory.mktemp("tls") / "ca.pem"
        ca.write_text(_exec(container, ["cat", "/tmp/tls/ca.pem"]))  # noqa: S108 — inside the container
        yield {
            "container": container,
            "host": container.get_container_host_ip(),
            "tls_port": int(container.get_exposed_port(5671)),
            "plain_port": int(container.get_exposed_port(5672)),
            "ca": str(ca),
        }


def _ssl_flags(rabbit: dict[str, Any]) -> list[str]:
    """``ssl`` column of every connection the broker currently has."""
    out = _exec(rabbit["container"], ["rabbitmqctl", "list_connections", "ssl", "--no-table-headers"])
    return [line.strip() for line in out.splitlines() if line.strip() in ("true", "false")]


def _security(rabbit: dict[str, Any], **ssl_kw: Any) -> Any:
    from rabbitkit.core.config import SecurityConfig, SSLConfig

    # server_hostname: the mapped host may be an IP or a name; the cert is for
    # "localhost", and pinning it also exercises SSLConfig.server_hostname.
    ssl_kw.setdefault("server_hostname", "localhost")
    return SecurityConfig(ssl=SSLConfig(enabled=True, ca_certs=rabbit["ca"], **ssl_kw))


def _conn(rabbit: dict[str, Any], port: int) -> Any:
    from rabbitkit.core.config import ConnectionConfig

    return ConnectionConfig(
        host=rabbit["host"], port=port, socket_timeout=5, reconnect_backoff_base=0.05, reconnect_backoff_max=0.1
    )


async def test_async_connection_is_encrypted(tls_rabbit: dict[str, Any]) -> None:
    from rabbitkit.async_.transport import AsyncTransportImpl
    from rabbitkit.core.topology import RabbitQueue
    from rabbitkit.core.types import MessageEnvelope

    transport = AsyncTransportImpl(
        connection_config=_conn(tls_rabbit, tls_rabbit["tls_port"]), security_config=_security(tls_rabbit)
    )
    await transport.connect()
    try:
        flags = await asyncio.to_thread(_ssl_flags, tls_rabbit)
        assert flags and all(f == "true" for f in flags), flags
        await transport.declare_queue(RabbitQueue(name="tls-q"))
        outcome = await transport.publish(MessageEnvelope(routing_key="tls-q", body=b"over tls"))
        assert outcome.ok, outcome
    finally:
        await transport.disconnect()


async def test_async_wrong_server_hostname_is_rejected(tls_rabbit: dict[str, Any]) -> None:
    from rabbitkit.async_.transport import AsyncTransportImpl

    transport = AsyncTransportImpl(
        connection_config=_conn(tls_rabbit, tls_rabbit["tls_port"]),
        security_config=_security(tls_rabbit, server_hostname="wrong.example"),
    )
    with pytest.raises((Exception, asyncio.TimeoutError)):  # any refusal; never a connection
        await asyncio.wait_for(transport.connect(), timeout=20)
    with contextlib.suppress(Exception):
        await transport.disconnect()


async def test_async_tls_against_a_plain_port_never_connects_unencrypted(tls_rabbit: dict[str, Any]) -> None:
    from rabbitkit.async_.transport import AsyncTransportImpl

    transport = AsyncTransportImpl(
        connection_config=_conn(tls_rabbit, tls_rabbit["plain_port"]), security_config=_security(tls_rabbit)
    )
    with pytest.raises((Exception, asyncio.TimeoutError)):
        await asyncio.wait_for(transport.connect(), timeout=20)
    with contextlib.suppress(Exception):
        await transport.disconnect()
    assert "false" not in await asyncio.to_thread(_ssl_flags, tls_rabbit)


def test_sync_connection_is_encrypted(tls_rabbit: dict[str, Any]) -> None:
    from rabbitkit.sync.transport import SyncTransport

    transport = SyncTransport(
        connection_config=_conn(tls_rabbit, tls_rabbit["tls_port"]), security_config=_security(tls_rabbit)
    )
    transport.connect()
    try:
        flags = _ssl_flags(tls_rabbit)
        assert flags and all(f == "true" for f in flags), flags
    finally:
        transport.disconnect()


def test_ca_file_was_written(tls_rabbit: dict[str, Any]) -> None:
    assert Path(tls_rabbit["ca"]).read_text().startswith("-----BEGIN CERTIFICATE-----")
