"""Tests for the management client's TLS options (``ca_certs``, ``certfile``, ``keyfile``)
against a real local HTTPS server signed by a private test CA (generated per run with
trustme: no key material is stored in the repository)."""

from __future__ import annotations

import json
import secrets
import ssl
import threading
import urllib.error
from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import pytest
import trustme

from rabbitkit.management import ManagementConfig, RabbitManagementClient


@pytest.fixture(scope="module")
def tls(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """A private CA, a server certificate for localhost and 127.0.0.1, a client certificate,
    and an unrelated CA, written as PEM files."""
    directory = tmp_path_factory.mktemp("management_tls")
    ca, other = trustme.CA(), trustme.CA()
    ca.cert_pem.write_to_path(str(directory / "ca.pem"))
    other.cert_pem.write_to_path(str(directory / "other-ca.pem"))
    ca.issue_cert("localhost", "127.0.0.1").private_key_and_cert_chain_pem.write_to_path(str(directory / "server.pem"))
    client_cert = ca.issue_cert("rabbitkit-test-client")
    client_cert.cert_chain_pems[0].write_to_path(str(directory / "client.pem"))
    client_cert.private_key_pem.write_to_path(str(directory / "client.key"))
    return directory


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path == "/api/redirect":
            self.send_response(302)
            self.send_header("Location", "https://internal.example/")
            self.end_headers()
            return
        body = json.dumps({"rabbitmq_version": "4.1.0", "management_version": "4.1.0"}).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:  # noqa: A002 - http.server API
        return


@contextmanager
def https_server(tls: Path, *, require_client_cert: bool = False) -> Iterator[str]:
    """An HTTPS server on 127.0.0.1 with a certificate from the private test CA."""
    server = ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.minimum_version = ssl.TLSVersion.TLSv1_2
    context.load_cert_chain(str(tls / "server.pem"))
    if require_client_cert:
        context.verify_mode = ssl.CERT_REQUIRED
        context.load_verify_locations(str(tls / "ca.pem"))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"https://localhost:{server.server_address[1]}"
    finally:
        server.shutdown()
        server.server_close()


def client(url: str, **tls: str) -> RabbitManagementClient:
    # A fresh random password: the test server does not check credentials.
    return RabbitManagementClient(
        ManagementConfig(url=url, username="admin", password=secrets.token_urlsafe(12), timeout=5.0, **tls)
    )


class TestTlsConfig:
    def test_tls_files_need_an_https_url(self) -> None:
        with pytest.raises(ValueError, match="https://"):
            ManagementConfig(url="http://rabbit:15672", username="admin", ca_certs="/ca.pem")

    def test_a_keyfile_needs_a_certfile(self) -> None:
        with pytest.raises(ValueError, match="keyfile needs certfile"):
            ManagementConfig(url="https://rabbit:15671", username="admin", keyfile="/client.key")

    def test_tls_options_default_to_none(self) -> None:
        config = ManagementConfig()
        assert (config.ca_certs, config.certfile, config.keyfile) == (None, None, None)

    def test_repr_still_masks_the_password(self) -> None:
        password = secrets.token_urlsafe(12)
        config = ManagementConfig(url="https://rabbit:15671", username="admin", password=password, ca_certs="/ca.pem")
        assert password not in repr(config)


class TestSyncTls:
    def test_a_private_ca_is_trusted_with_ca_certs(self, tls: Path) -> None:
        with https_server(tls) as url:
            assert client(url, ca_certs=str(tls / "ca.pem")).overview()["rabbitmq_version"] == "4.1.0"

    def test_without_ca_certs_the_private_ca_is_refused(self, tls: Path) -> None:
        with https_server(tls) as url, pytest.raises(urllib.error.URLError, match="CERTIFICATE_VERIFY_FAILED"):
            client(url).overview()

    def test_a_server_signed_by_another_ca_is_refused(self, tls: Path) -> None:
        with https_server(tls) as url, pytest.raises(urllib.error.URLError, match="CERTIFICATE_VERIFY_FAILED"):
            client(url, ca_certs=str(tls / "other-ca.pem")).overview()

    def test_the_client_certificate_is_presented(self, tls: Path) -> None:
        with https_server(tls, require_client_cert=True) as url:
            mtls = client(
                url,
                ca_certs=str(tls / "ca.pem"),
                certfile=str(tls / "client.pem"),
                keyfile=str(tls / "client.key"),
            )
            assert mtls.overview()["rabbitmq_version"] == "4.1.0"

    def test_without_a_client_certificate_mutual_tls_fails(self, tls: Path) -> None:
        refused = (urllib.error.URLError, ssl.SSLError, OSError)
        with https_server(tls, require_client_cert=True) as url, pytest.raises(refused):
            client(url, ca_certs=str(tls / "ca.pem")).overview()

    def test_redirects_are_still_refused_over_tls(self, tls: Path) -> None:
        with https_server(tls) as url, pytest.raises(ValueError, match="Unexpected redirect"):
            client(url, ca_certs=str(tls / "ca.pem"))._request("GET", "/redirect")


class TestAsyncTls:
    async def test_a_private_ca_is_trusted_with_ca_certs(self, tls: Path) -> None:
        with https_server(tls) as url:
            management = client(url, ca_certs=str(tls / "ca.pem"))
            try:
                assert (await management.overview_async())["rabbitmq_version"] == "4.1.0"
            finally:
                await management.aclose()

    async def test_without_ca_certs_the_private_ca_is_refused(self, tls: Path) -> None:
        import aiohttp

        with https_server(tls) as url:
            management = client(url)
            try:
                with pytest.raises(aiohttp.ClientConnectorCertificateError):
                    await management.overview_async()
            finally:
                await management.aclose()
