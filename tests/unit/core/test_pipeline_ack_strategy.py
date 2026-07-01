"""Tests for the AckPolicy strategy dispatch (R1).

The if/elif chains over AckPolicy were replaced by a Strategy dispatch
(``_ACK_STRATEGIES`` / ``_ACK_STRATEGIES_ASYNC``). These tests pin the strategy
objects directly so the dispatch mapping and per-policy settlement are covered
independently of the pipeline orchestration in test_pipeline.py.
"""

from __future__ import annotations

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.pipeline import _ACK_STRATEGIES, _ACK_STRATEGIES_ASYNC
from rabbitkit.core.types import AckPolicy

# ── helpers ───────────────────────────────────────────────────────────────


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {"body": b'{"id": 1}', "routing_key": "orders.created"}
    defaults.update(kwargs)
    return RabbitMessage(**defaults)  # type: ignore[arg-type]


def _wire_sync(msg: RabbitMessage) -> tuple[object, object, object]:
    from unittest.mock import MagicMock

    ack = MagicMock()
    nack = MagicMock()
    reject = MagicMock()
    msg._ack_fn = ack
    msg._nack_fn = nack
    msg._reject_fn = reject
    return ack, nack, reject


def _wire_async(msg: RabbitMessage) -> tuple[object, object, object]:
    from unittest.mock import AsyncMock

    ack = AsyncMock()
    nack = AsyncMock()
    reject = AsyncMock()
    msg._ack_async_fn = ack
    msg._nack_async_fn = nack
    msg._reject_async_fn = reject
    return ack, nack, reject


# ── dispatch mapping ─────────────────────────────────────────────────────


class TestAckStrategyMapping:
    def test_sync_strategies_cover_all_policies(self) -> None:
        assert set(_ACK_STRATEGIES) == set(AckPolicy)

    def test_async_strategies_cover_all_policies(self) -> None:
        assert set(_ACK_STRATEGIES_ASYNC) == set(AckPolicy)

    def test_only_ack_first_pre_acks(self) -> None:
        assert _ACK_STRATEGIES[AckPolicy.ACK_FIRST].acks_first is True
        for policy in (AckPolicy.AUTO, AckPolicy.MANUAL, AckPolicy.NACK_ON_ERROR):
            assert _ACK_STRATEGIES[policy].acks_first is False
            assert _ACK_STRATEGIES_ASYNC[policy].acks_first is False


# ── sync strategies ───────────────────────────────────────────────────────


class TestSyncAckStrategies:
    def test_auto_on_success_acks_when_unset(self) -> None:
        msg = _make_message()
        ack, _, _ = _wire_sync(msg)
        _ACK_STRATEGIES[AckPolicy.AUTO].on_success(msg)
        ack.assert_called_once()  # type: ignore[union-attr]
        assert msg._disposition == "acked"

    def test_on_success_noop_when_already_settled(self) -> None:
        msg = _make_message()
        ack, _, _ = _wire_sync(msg)
        msg.ack()  # pre-settle
        _ACK_STRATEGIES[AckPolicy.AUTO].on_success(msg)
        # No second ack — idempotent guard held by is_settled.
        ack.assert_called_once()  # type: ignore[union-attr]

    def test_auto_on_error_transient_nacks_with_requeue(self) -> None:
        msg = _make_message()
        _, nack, reject = _wire_sync(msg)
        _ACK_STRATEGIES[AckPolicy.AUTO].on_error(msg, ConnectionResetError("boom"))
        nack.assert_called_once_with(True)  # type: ignore[union-attr]
        reject.assert_not_called()  # type: ignore[union-attr]
        assert msg._disposition == "nacked"

    def test_auto_on_error_permanent_rejects(self) -> None:
        msg = _make_message()
        _, _, reject = _wire_sync(msg)
        _ACK_STRATEGIES[AckPolicy.AUTO].on_error(msg, ValueError("bad"))
        reject.assert_called_once_with(False)  # type: ignore[union-attr]
        assert msg._disposition == "rejected"

    def test_manual_on_error_reraises_without_settling(self) -> None:
        msg = _make_message()
        _wire_sync(msg)
        # on_error uses a bare ``raise`` which re-raises the active exception;
        # in the pipeline this runs inside an ``except`` block, so wrap it here.
        try:
            raise RuntimeError("handler bug")
        except RuntimeError as exc:
            with pytest.raises(RuntimeError, match="handler bug"):
                _ACK_STRATEGIES[AckPolicy.MANUAL].on_error(msg, exc)
        # MANUAL never settles on the pipeline's behalf.
        assert msg._disposition == "pending"

    def test_nack_on_error_on_error_nacks_no_requeue(self) -> None:
        msg = _make_message()
        _, nack, reject = _wire_sync(msg)
        _ACK_STRATEGIES[AckPolicy.NACK_ON_ERROR].on_error(msg, RuntimeError("fail"))
        nack.assert_called_once_with(False)  # type: ignore[union-attr]
        reject.assert_not_called()  # type: ignore[union-attr]
        assert msg._disposition == "nacked"

    def test_ack_first_on_error_classifies_like_auto(self) -> None:
        # Defensive: ACK_FIRST.on_error mirrors AUTO classification if ever
        # called on an unsettled message (normally unreachable — pre-acked).
        msg = _make_message()
        _, nack, _ = _wire_sync(msg)
        _ACK_STRATEGIES[AckPolicy.ACK_FIRST].on_error(msg, ConnectionResetError("x"))
        nack.assert_called_once_with(True)  # type: ignore[union-attr]


# ── async strategies ──────────────────────────────────────────────────────


class TestAsyncAckStrategies:
    @pytest.mark.asyncio
    async def test_async_auto_on_success_acks(self) -> None:
        msg = _make_message()
        ack, _, _ = _wire_async(msg)
        await _ACK_STRATEGIES_ASYNC[AckPolicy.AUTO].on_success(msg)
        ack.assert_called_once()  # type: ignore[union-attr]

    @pytest.mark.asyncio
    async def test_async_auto_on_error_transient_nacks_with_requeue(self) -> None:
        msg = _make_message()
        _, nack, reject = _wire_async(msg)
        await _ACK_STRATEGIES_ASYNC[AckPolicy.AUTO].on_error(msg, ConnectionResetError("boom"))
        nack.assert_called_once_with(True)  # type: ignore[union-attr]
        reject.assert_not_called()  # type: ignore[union-attr]

    @pytest.mark.asyncio
    async def test_async_auto_on_error_permanent_rejects(self) -> None:
        msg = _make_message()
        _, _, reject = _wire_async(msg)
        await _ACK_STRATEGIES_ASYNC[AckPolicy.AUTO].on_error(msg, ValueError("bad"))
        reject.assert_called_once_with(False)  # type: ignore[union-attr]

    @pytest.mark.asyncio
    async def test_async_manual_on_error_reraises(self) -> None:
        msg = _make_message()
        _wire_async(msg)
        try:
            raise ValueError("async")
        except ValueError as exc:
            with pytest.raises(ValueError, match="async"):
                await _ACK_STRATEGIES_ASYNC[AckPolicy.MANUAL].on_error(msg, exc)
        assert msg._disposition == "pending"

    @pytest.mark.asyncio
    async def test_async_nack_on_error_on_error_nacks_no_requeue(self) -> None:
        msg = _make_message()
        _, nack, _ = _wire_async(msg)
        await _ACK_STRATEGIES_ASYNC[AckPolicy.NACK_ON_ERROR].on_error(msg, RuntimeError("fail"))
        nack.assert_called_once_with(False)  # type: ignore[union-attr]
