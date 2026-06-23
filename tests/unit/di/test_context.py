"""Tests for di/context.py — Context, Header, Path markers + ContextRepo."""

from __future__ import annotations

import threading

from rabbitkit.di.context import Context, ContextRepo, Header, Path

# ── Context marker ──────────────────────────────────────────────────────


class TestContextMarker:
    def test_create(self) -> None:
        ctx = Context("app")
        assert ctx.key == "app"

    def test_repr(self) -> None:
        ctx = Context("app")
        assert repr(ctx) == "Context('app')"

    def test_equality(self) -> None:
        assert Context("app") == Context("app")
        assert Context("app") != Context("other")
        assert Context("app") != "not a context"

    def test_hashable(self) -> None:
        s = {Context("app"), Context("app")}
        assert len(s) == 1


# ── Header marker ──────────────────────────────────────────────────────


class TestHeaderMarker:
    def test_create(self) -> None:
        h = Header("x-tenant")
        assert h.name == "x-tenant"

    def test_repr(self) -> None:
        h = Header("x-tenant")
        assert repr(h) == "Header('x-tenant')"

    def test_equality(self) -> None:
        assert Header("x-tenant") == Header("x-tenant")
        assert Header("x-tenant") != Header("x-other")
        assert Header("x-tenant") != "not a header"

    def test_hashable(self) -> None:
        s = {Header("x-tenant"), Header("x-tenant")}
        assert len(s) == 1


# ── Path marker ────────────────────────────────────────────────────────


class TestPathMarker:
    def test_create(self) -> None:
        p = Path("level")
        assert p.segment == "level"

    def test_repr(self) -> None:
        p = Path("level")
        assert repr(p) == "Path('level')"

    def test_equality(self) -> None:
        assert Path("level") == Path("level")
        assert Path("level") != Path("other")
        assert Path("level") != "not a path"

    def test_hashable(self) -> None:
        s = {Path("level"), Path("level")}
        assert len(s) == 1


# ── ContextRepo ─────────────────────────────────────────────────────────


class TestContextRepo:
    def test_set_get_global(self) -> None:
        repo = ContextRepo()
        repo.set_global("app", "my-app")
        assert repo.get("app") == "my-app"

    def test_get_missing_default(self) -> None:
        repo = ContextRepo()
        assert repo.get("missing") is None
        assert repo.get("missing", "default") == "default"

    def test_set_get_local(self) -> None:
        repo = ContextRepo()
        repo.set_local("request_id", "req-123")
        assert repo.get("request_id") == "req-123"

    def test_local_overrides_global(self) -> None:
        repo = ContextRepo()
        repo.set_global("key", "global-value")
        repo.set_local("key", "local-value")
        assert repo.get("key") == "local-value"

    def test_clear_local(self) -> None:
        repo = ContextRepo()
        repo.set_global("key", "global")
        repo.set_local("key", "local")
        repo.clear_local()
        assert repo.get("key") == "global"

    def test_has_global(self) -> None:
        repo = ContextRepo()
        repo.set_global("key", "value")
        assert repo.has("key") is True
        assert repo.has("missing") is False

    def test_has_local(self) -> None:
        repo = ContextRepo()
        repo.set_local("key", "value")
        assert repo.has("key") is True

    def test_thread_isolation(self) -> None:
        repo = ContextRepo()
        repo.set_global("shared", "global")
        results: dict[str, str | None] = {}

        def thread_work(name: str) -> None:
            repo.set_local("thread_key", name)
            results[name] = repo.get("thread_key")

        t1 = threading.Thread(target=thread_work, args=("t1",))
        t2 = threading.Thread(target=thread_work, args=("t2",))
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert results["t1"] == "t1"
        assert results["t2"] == "t2"
