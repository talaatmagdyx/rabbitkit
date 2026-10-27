"""DLQ replay/purge authorization and audit (DLQInspector ``authorizer``,
CLI ``--require-actor``), and the replay-loop limit composed with it."""

from __future__ import annotations

import logging
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from typer.testing import CliRunner

from rabbitkit.cli import app
from rabbitkit.core.errors import DLQOperationDeniedError
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope, PublishOutcome, PublishStatus
from rabbitkit.dlq import DLQInspector, ReplayRequest, default_actor

AUDIT = "rabbitkit.dlq.audit"


def _msg(**headers: Any) -> RabbitMessage:
    msg = RabbitMessage(
        body=b"m", routing_key="orders.dlq", headers={"x-rabbitkit-original-queue": "orders", **headers}
    )
    msg._ack_fn, msg._nack_fn = MagicMock(), MagicMock()

    async def _settle(*_a: Any) -> None:
        return None

    msg._ack_async_fn = MagicMock(side_effect=_settle)
    msg._nack_async_fn = MagicMock(side_effect=_settle)
    return msg


class _Transport:
    def __init__(self, messages: list[RabbitMessage]) -> None:
        self.queue = list(messages)
        self.published: list[MessageEnvelope] = []
        self.gets = 0
        self.purged: list[str] = []

    def basic_get(self, queue: str) -> RabbitMessage | None:
        self.gets += 1
        return self.queue.pop(0) if self.queue else None

    def publish(self, envelope: MessageEnvelope) -> PublishOutcome:
        self.published.append(envelope)
        return PublishOutcome(status=PublishStatus.CONFIRMED)

    def purge_queue(self, queue: str) -> int:
        self.purged.append(queue)
        return 4


class _AsyncTransport(_Transport):
    async def basic_get(self, queue: str) -> RabbitMessage | None:  # type: ignore[override]
        return _Transport.basic_get(self, queue)

    async def publish(self, envelope: MessageEnvelope) -> PublishOutcome:  # type: ignore[override]
        return _Transport.publish(self, envelope)

    async def purge_queue(self, queue: str) -> int:  # type: ignore[override]
        return _Transport.purge_queue(self, queue)


def _audit(caplog: pytest.LogCaptureFixture) -> str:
    return "\n".join(r.getMessage() for r in caplog.records if r.name == AUDIT)


class TestInspectorAuthorizer:
    def test_allowed_replay_sees_the_whole_request(self) -> None:
        seen: list[ReplayRequest] = []

        def allow(request: ReplayRequest) -> bool:
            seen.append(request)
            return True

        def only_timeouts(m: RabbitMessage) -> bool:
            return True

        transport = _Transport([_msg()])
        result = DLQInspector(transport, authorizer=allow).replay(
            "orders.dlq", predicate=only_timeouts, actor="alice", limit=5, reset_retry_count=True
        )
        assert int(result) == 1
        (request,) = seen
        assert request.operation == "replay" and request.actor == "alice" and request.actor_explicit
        assert request.queue == "orders.dlq" and "original queue" in request.target
        assert request.limit == 5 and request.reset_retry_count and request.max_replays == 3
        assert "only_timeouts" in request.predicate

    def test_denied_replay_reads_nothing_and_is_audited(self, caplog: pytest.LogCaptureFixture) -> None:
        transport = _Transport([_msg()])
        inspector = DLQInspector(transport, authorizer=lambda r: r.actor_explicit)
        with caplog.at_level(logging.INFO, logger=AUDIT), pytest.raises(DLQOperationDeniedError):
            inspector.replay("orders.dlq")
        assert transport.gets == 0 and transport.published == []
        assert "DLQ replay DENIED" in _audit(caplog) and "authorizer refused" in _audit(caplog)

    def test_a_raising_authorizer_denies(self, caplog: pytest.LogCaptureFixture) -> None:
        def broken(request: ReplayRequest) -> bool:
            raise RuntimeError("policy store down")

        transport = _Transport([_msg()])
        with caplog.at_level(logging.INFO, logger=AUDIT), pytest.raises(DLQOperationDeniedError) as info:
            DLQInspector(transport, authorizer=broken).replay("orders.dlq", actor="bob")
        assert isinstance(info.value.__cause__, RuntimeError)
        assert transport.gets == 0
        assert "authorizer raised RuntimeError" in _audit(caplog)

    async def test_async_replay_is_authorized_too(self) -> None:
        transport = _AsyncTransport([_msg()])
        with pytest.raises(DLQOperationDeniedError):
            await DLQInspector(transport, authorizer=lambda r: False).replay_async("orders.dlq", actor="x")
        assert transport.gets == 0
        assert int(await DLQInspector(transport, authorizer=lambda r: True).replay_async("orders.dlq")) == 1

    def test_lifting_the_replay_limit_is_visible_to_the_authorizer(self) -> None:
        seen: list[ReplayRequest] = []
        transport = _Transport([_msg(**{"x-rabbitkit-replay-count": 7})])
        inspector = DLQInspector(transport, authorizer=lambda r: seen.append(r) is None)
        assert int(inspector.replay("orders.dlq", max_replays=None)) == 1
        assert seen[0].max_replays is None

    def test_purge_is_authorized_and_audited(self, caplog: pytest.LogCaptureFixture) -> None:
        transport = _Transport([])
        with caplog.at_level(logging.INFO, logger=AUDIT):
            assert DLQInspector(transport, authorizer=lambda r: r.operation == "purge").purge("q.dlq", actor="ops") == 4
        assert "DLQ purge: actor=ops queue='q.dlq'" in _audit(caplog)
        with pytest.raises(DLQOperationDeniedError):
            DLQInspector(transport, authorizer=lambda r: False).purge("q.dlq")
        assert transport.purged == ["q.dlq"]

    async def test_async_purge(self) -> None:
        transport = _AsyncTransport([])
        assert await DLQInspector(transport).purge_async("q.dlq") == 4
        with pytest.raises(DLQOperationDeniedError):
            await DLQInspector(transport, authorizer=lambda r: False).purge_async("q.dlq")

    def test_no_authorizer_allows_everything(self) -> None:
        transport = _Transport([_msg()])
        assert int(DLQInspector(transport).replay("orders.dlq")) == 1


def test_default_actor_falls_back_when_the_os_user_is_unknown() -> None:
    with patch("rabbitkit.dlq.getpass.getuser", side_effect=OSError("no passwd entry")):
        assert default_actor() == "unknown"


# ── CLI --require-actor ───────────────────────────────────────────────────

runner = CliRunner()


def _cli(args: list[str], env: dict[str, str] | None = None) -> tuple[Any, MagicMock]:
    mock_pika = MagicMock()
    channel = mock_pika.BlockingConnection.return_value.channel.return_value
    channel.basic_get.return_value = (None, None, None)
    with patch.dict("sys.modules", {"pika": mock_pika, "pika.exceptions": mock_pika.exceptions}):
        result = runner.invoke(app, ["dlq", "replay", *args], env=env or {})
    return result, mock_pika


class TestCliRequireActor:
    def test_refused_without_an_actor(self, caplog: pytest.LogCaptureFixture) -> None:
        with caplog.at_level(logging.INFO, logger=AUDIT):
            result, mock_pika = _cli(["orders.dlq", "", "--require-actor"])
        assert result.exit_code == 2
        assert "--require-actor" in result.output
        mock_pika.BlockingConnection.assert_not_called()
        assert "DLQ replay DENIED" in _audit(caplog)

    def test_env_var_turns_it_on(self) -> None:
        result, mock_pika = _cli(["orders.dlq", ""], env={"RABBITKIT_REPLAY_REQUIRE_ACTOR": "1"})
        assert result.exit_code == 2
        mock_pika.BlockingConnection.assert_not_called()

    def test_allowed_with_an_actor(self) -> None:
        result, mock_pika = _cli(["orders.dlq", "", "--require-actor", "--actor", "alice"])
        assert result.exit_code == 0, result.output
        mock_pika.BlockingConnection.assert_called_once()

    def test_dry_run_needs_no_actor(self) -> None:
        result, _ = _cli(["orders.dlq", "", "--require-actor", "--dry-run"])
        assert result.exit_code == 0, result.output
