"""Tests for monitoring dashboard (F16)."""

from __future__ import annotations

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
