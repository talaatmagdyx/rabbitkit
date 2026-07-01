"""Tests for monitoring dashboard (F16)."""

from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

try:
    from starlette.testclient import TestClient

    _STARLETTE_AVAILABLE = True
except ImportError:
    _STARLETTE_AVAILABLE = False

from rabbitkit.core.topology import RabbitQueue
from rabbitkit.core.types import AckPolicy


def _make_mock_broker(routes=None):
    broker = MagicMock()
    if routes is None:
        route = MagicMock()
        route.name = "test-route"
        route.queue = RabbitQueue(name="test-queue")
        route.exchange = None
        route.ack_policy = AckPolicy.AUTO
        route.tags = frozenset()
        route.description = "Test route"
        routes = [route]
    broker.routes = routes
    broker._started = False
    broker._transport = None
    broker._worker_pool = None
    return broker


@pytest.mark.skipif(not _STARLETTE_AVAILABLE, reason="starlette not installed")
class TestDashboard:
    def test_index_returns_html(self):
        from rabbitkit.dashboard import create_dashboard_app

        broker = _make_mock_broker()
        app = create_dashboard_app(broker)
        client = TestClient(app)
        resp = client.get("/")
        assert resp.status_code == 200
        assert "rabbitkit Dashboard" in resp.text

    def test_route_names_are_html_escaped(self):
        """Regression: a queue/route name with HTML must not inject markup (XSS)."""
        from rabbitkit.dashboard import create_dashboard_app

        route = MagicMock()
        route.name = "<script>alert(1)</script>"
        route.queue = RabbitQueue(name="<img src=x onerror=alert(2)>")
        route.exchange = None
        route.ack_policy = AckPolicy.AUTO
        broker = _make_mock_broker(routes=[route])
        client = TestClient(create_dashboard_app(broker))
        resp = client.get("/")
        assert resp.status_code == 200
        assert "<script>alert(1)</script>" not in resp.text  # raw markup not present
        assert "&lt;script&gt;alert(1)&lt;/script&gt;" in resp.text  # escaped instead

    def test_api_health(self):
        from rabbitkit.dashboard import create_dashboard_app

        broker = _make_mock_broker()
        app = create_dashboard_app(broker)
        client = TestClient(app)
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert "status" in data

    def test_api_routes(self):
        from rabbitkit.dashboard import create_dashboard_app

        broker = _make_mock_broker()
        app = create_dashboard_app(broker)
        client = TestClient(app)
        resp = client.get("/api/routes")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data) == 1
        assert data[0]["name"] == "test-route"

    def test_empty_routes(self):
        from rabbitkit.dashboard import create_dashboard_app

        broker = _make_mock_broker(routes=[])
        app = create_dashboard_app(broker)
        client = TestClient(app)
        resp = client.get("/api/routes")
        assert resp.status_code == 200
        assert resp.json() == []


@pytest.mark.skipif(not _STARLETTE_AVAILABLE, reason="starlette not installed")
class TestDashboardAuth:
    def test_without_auth_token_passes_and_warns(self, caplog: pytest.LogCaptureFixture):
        """M-3: no auth_token -> requests pass and a startup warning is emitted."""
        from rabbitkit.dashboard import create_dashboard_app

        broker = _make_mock_broker()
        with caplog.at_level(logging.WARNING, logger="rabbitkit.dashboard.app"):
            app = create_dashboard_app(broker)
        client = TestClient(app)
        resp = client.get("/api/health")
        assert resp.status_code == 200
        assert any("WITHOUT authentication" in rec.message for rec in caplog.records)

    def test_with_auth_token_no_header_returns_401(self):
        from rabbitkit.dashboard import create_dashboard_app

        broker = _make_mock_broker()
        app = create_dashboard_app(broker, auth_token="s3cr3t")
        client = TestClient(app)
        resp = client.get("/api/health")
        assert resp.status_code == 401

    def test_with_auth_token_wrong_bearer_returns_401(self):
        from rabbitkit.dashboard import create_dashboard_app

        broker = _make_mock_broker()
        app = create_dashboard_app(broker, auth_token="s3cr3t")
        client = TestClient(app)
        resp = client.get("/api/health", headers={"Authorization": "Bearer wrong"})
        assert resp.status_code == 401

    def test_with_auth_token_correct_bearer_returns_200(self):
        from rabbitkit.dashboard import create_dashboard_app

        broker = _make_mock_broker()
        app = create_dashboard_app(broker, auth_token="s3cr3t")
        client = TestClient(app)
        resp = client.get("/api/health", headers={"Authorization": "Bearer s3cr3t"})
        assert resp.status_code == 200

    def test_with_auth_token_protects_all_routes(self):
        from rabbitkit.dashboard import create_dashboard_app

        broker = _make_mock_broker()
        app = create_dashboard_app(broker, auth_token="tok")
        client = TestClient(app)
        for path in ("/", "/api/health", "/api/routes"):
            assert client.get(path).status_code == 401, path
            assert client.get(path, headers={"Authorization": "Bearer tok"}).status_code == 200, path

    def test_with_auth_token_does_not_warn(self, caplog: pytest.LogCaptureFixture):
        """M-3: setting auth_token suppresses the unauthenticated-startup warning."""
        from rabbitkit.dashboard import create_dashboard_app

        broker = _make_mock_broker()
        with caplog.at_level(logging.WARNING, logger="rabbitkit.dashboard.app"):
            create_dashboard_app(broker, auth_token="s3cr3t")
        assert not any("WITHOUT authentication" in rec.message for rec in caplog.records)
