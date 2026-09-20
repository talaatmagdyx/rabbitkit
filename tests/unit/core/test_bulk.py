"""Tests for core/bulk.py — bulk publish contract, outcome mapping, preparation, budgets."""

from __future__ import annotations

import asyncio
import dataclasses
import threading
import time

import pytest

from rabbitkit.core.bulk import (
    ALL_REASONS,
    REASON_ADMISSION_TIMEOUT,
    REASON_BACKPRESSURE,
    REASON_BODY_TOO_LARGE,
    REASON_CONFIRM_TIMEOUT,
    REASON_CONFIRMED,
    REASON_EXCEEDS_BUFFER,
    REASON_INVALID_ENVELOPE,
    REASON_INVALID_HEADERS,
    REASON_INVALID_TYPE,
    REASON_NACKED,
    REASON_NO_OUTCOME,
    REASON_NOT_STARTED,
    REASON_PUBLISH_ERROR,
    REASON_RETURNED,
    REASON_SENT_UNCONFIRMED,
    AsyncByteBudget,
    BulkPublishError,
    BulkPublishItem,
    BulkPublishOptions,
    BulkPublishResult,
    ByteBudget,
    PublishPreparer,
    classify_publish_exception,
    classify_publish_outcome,
    summarize_statuses,
)
from rabbitkit.core.errors import (
    BackpressureError,
    BrokerNotStartedError,
    ConfigValidationError,
    MessageTooLargeError,
)
from rabbitkit.core.types import BulkPublishStatus, MessageEnvelope, PublishOutcome, PublishStatus


def _item(index: int, status: BulkPublishStatus, reason: str = REASON_CONFIRMED) -> BulkPublishItem:
    return BulkPublishItem(index=index, status=status, reason=reason)


# ── BulkPublishOptions ─────────────────────────────────────────────────────


class TestBulkPublishOptions:
    def test_defaults_are_bounded(self) -> None:
        o = BulkPublishOptions()
        assert o.max_in_flight >= 1
        assert o.max_buffer_bytes >= 1
        assert o.admission_timeout > 0
        assert o.overall_timeout is not None and o.overall_timeout > 0
        assert o.max_items >= 1

    @pytest.mark.parametrize(
        "kwargs",
        [
            {"max_in_flight": 0},
            {"max_buffer_bytes": 0},
            {"admission_timeout": 0},
            {"confirm_timeout": 0},
            {"overall_timeout": 0},
            {"drain_grace": -1},
            {"max_items": 0},
        ],
    )
    def test_rejects_non_positive_bounds(self, kwargs: dict[str, float]) -> None:
        with pytest.raises(ConfigValidationError):
            BulkPublishOptions(**kwargs)  # type: ignore[arg-type]

    def test_none_timeouts_allowed(self) -> None:
        o = BulkPublishOptions(confirm_timeout=None, overall_timeout=None)
        assert o.confirm_timeout is None
        assert o.overall_timeout is None


# ── BulkPublishItem / Result ───────────────────────────────────────────────


class TestBulkPublishItem:
    def test_reason_must_be_bounded_code(self) -> None:
        with pytest.raises(ValueError, match="bounded reason code"):
            BulkPublishItem(index=0, status=BulkPublishStatus.UNKNOWN, reason="ConnectionResetError: secret")

    def test_ok_only_for_confirmed(self) -> None:
        assert _item(0, BulkPublishStatus.CONFIRMED).ok
        for s in BulkPublishStatus:
            if s is not BulkPublishStatus.CONFIRMED:
                assert not _item(0, s, REASON_PUBLISH_ERROR).ok

    def test_unknown_is_never_safe_to_resubmit(self) -> None:
        assert not _item(0, BulkPublishStatus.UNKNOWN, REASON_CONFIRM_TIMEOUT).safe_to_resubmit
        assert _item(0, BulkPublishStatus.NOT_SENT, REASON_ADMISSION_TIMEOUT).safe_to_resubmit
        assert _item(0, BulkPublishStatus.INVALID, REASON_BODY_TOO_LARGE).safe_to_resubmit
        assert not _item(0, BulkPublishStatus.NACKED, REASON_NACKED).safe_to_resubmit

    def test_duration(self) -> None:
        it = BulkPublishItem(
            index=0, status=BulkPublishStatus.CONFIRMED, reason=REASON_CONFIRMED, submitted_at=1.0, settled_at=1.5
        )
        assert it.duration == pytest.approx(0.5)
        assert _item(0, BulkPublishStatus.CONFIRMED).duration is None


class TestBulkPublishResult:
    def test_requires_input_order_and_unique_indices(self) -> None:
        with pytest.raises(ValueError):
            BulkPublishResult(items=(_item(1, BulkPublishStatus.CONFIRMED), _item(0, BulkPublishStatus.CONFIRMED)))
        with pytest.raises(ValueError):
            BulkPublishResult(items=(_item(0, BulkPublishStatus.CONFIRMED), _item(0, BulkPublishStatus.CONFIRMED)))

    def test_counts_and_filters(self) -> None:
        r = BulkPublishResult(
            items=(
                _item(0, BulkPublishStatus.CONFIRMED),
                _item(1, BulkPublishStatus.UNKNOWN, REASON_CONFIRM_TIMEOUT),
                _item(2, BulkPublishStatus.NOT_SENT, REASON_ADMISSION_TIMEOUT),
                _item(3, BulkPublishStatus.UNROUTABLE, REASON_RETURNED),
            )
        )
        assert len(r) == 4
        assert r.counts == {
            BulkPublishStatus.CONFIRMED: 1,
            BulkPublishStatus.UNKNOWN: 1,
            BulkPublishStatus.NOT_SENT: 1,
            BulkPublishStatus.UNROUTABLE: 1,
        }
        assert [it.index for it in r.confirmed] == [0]
        assert [it.index for it in r.unknown] == [1]
        assert [it.index for it in r.resubmittable] == [2]
        assert [it.index for it in r.failed] == [1, 2, 3]
        assert not r.all_confirmed
        assert summarize_statuses(r.items) == {"confirmed": 1, "unknown": 1, "not_sent": 1, "unroutable": 1}

    def test_raise_for_status(self) -> None:
        ok = BulkPublishResult(items=(_item(0, BulkPublishStatus.CONFIRMED),))
        assert ok.raise_for_status() is ok
        bad = BulkPublishResult(items=(_item(0, BulkPublishStatus.NACKED, REASON_NACKED),))
        with pytest.raises(BulkPublishError) as ei:
            bad.raise_for_status()
        assert ei.value.result is bad
        assert "nacked=1" in str(ei.value)

    def test_empty_result_is_all_confirmed(self) -> None:
        assert BulkPublishResult(items=()).all_confirmed


# ── Outcome mapping ────────────────────────────────────────────────────────


class TestClassifyPublishOutcome:
    @pytest.mark.parametrize(
        ("status", "error", "expected", "reason"),
        [
            (PublishStatus.CONFIRMED, None, BulkPublishStatus.CONFIRMED, REASON_CONFIRMED),
            (PublishStatus.SENT, None, BulkPublishStatus.UNKNOWN, REASON_SENT_UNCONFIRMED),
            (PublishStatus.NACKED, None, BulkPublishStatus.NACKED, REASON_NACKED),
            (PublishStatus.TIMEOUT, None, BulkPublishStatus.UNKNOWN, REASON_CONFIRM_TIMEOUT),
            (PublishStatus.RETURNED, None, BulkPublishStatus.UNROUTABLE, REASON_RETURNED),
            (PublishStatus.ERROR, RuntimeError("boom"), BulkPublishStatus.UNKNOWN, REASON_PUBLISH_ERROR),
            (PublishStatus.ERROR, None, BulkPublishStatus.UNKNOWN, REASON_PUBLISH_ERROR),
            (PublishStatus.ERROR, BackpressureError("x"), BulkPublishStatus.NOT_SENT, REASON_BACKPRESSURE),
            (PublishStatus.ERROR, BrokerNotStartedError("x"), BulkPublishStatus.NOT_SENT, REASON_NOT_STARTED),
            (PublishStatus.ERROR, MessageTooLargeError("x"), BulkPublishStatus.INVALID, REASON_BODY_TOO_LARGE),
            (PublishStatus.ERROR, ValueError("x"), BulkPublishStatus.INVALID, REASON_INVALID_ENVELOPE),
            (PublishStatus.ERROR, TypeError("x"), BulkPublishStatus.INVALID, REASON_INVALID_ENVELOPE),
        ],
    )
    def test_mapping(
        self, status: PublishStatus, error: BaseException | None, expected: BulkPublishStatus, reason: str
    ) -> None:
        got = classify_publish_outcome(PublishOutcome(status=status, error=error))
        assert got == (expected, reason)
        assert reason in ALL_REASONS

    def test_none_outcome_is_unknown(self) -> None:
        assert classify_publish_outcome(None) == (BulkPublishStatus.UNKNOWN, REASON_NO_OUTCOME)

    def test_sent_is_never_confirmed(self) -> None:
        """Safety invariant 2: fire-and-forget must not be reported as confirmed."""
        status, _ = classify_publish_outcome(PublishOutcome(status=PublishStatus.SENT))
        assert status is BulkPublishStatus.UNKNOWN

    def test_returned_beats_ok_flag(self) -> None:
        """An unroutable message is not a successful routed publication even
        though the broker confirms it after the Basic.Return."""
        status, _ = classify_publish_outcome(PublishOutcome(status=PublishStatus.RETURNED))
        assert status is BulkPublishStatus.UNROUTABLE


class TestClassifyPublishException:
    def test_cancelled_is_unknown(self) -> None:
        assert classify_publish_exception(asyncio.CancelledError())[0] is BulkPublishStatus.UNKNOWN

    def test_timeout_is_unknown(self) -> None:
        assert classify_publish_exception(TimeoutError()) == (BulkPublishStatus.UNKNOWN, REASON_CONFIRM_TIMEOUT)

    def test_pre_submission_errors_are_not_sent_or_invalid(self) -> None:
        assert classify_publish_exception(BackpressureError("x"))[0] is BulkPublishStatus.NOT_SENT
        assert classify_publish_exception(BrokerNotStartedError("x"))[0] is BulkPublishStatus.NOT_SENT
        assert classify_publish_exception(MessageTooLargeError("x"))[0] is BulkPublishStatus.INVALID
        assert classify_publish_exception(ValueError("x"))[0] is BulkPublishStatus.INVALID

    def test_generic_exception_is_unknown(self) -> None:
        assert classify_publish_exception(ConnectionResetError())[0] is BulkPublishStatus.UNKNOWN


# ── PublishPreparer ────────────────────────────────────────────────────────


class TestPublishPreparer:
    def _prep(self, **kw: int) -> PublishPreparer:
        kw.setdefault("max_message_bytes", 1024)
        kw.setdefault("max_buffer_bytes", 4096)
        return PublishPreparer(**kw)

    def test_valid_envelope_passes_and_is_copied(self) -> None:
        headers = {"x-a": 1}
        env = MessageEnvelope(routing_key="q", body=b"x", headers=headers)
        prepared, invalid = self._prep().prepare(0, env)
        assert invalid is None and prepared is not None
        headers["x-a"] = 2  # caller mutates after admission
        assert prepared.headers == {"x-a": 1}
        assert prepared.body == b"x" and prepared.routing_key == "q"

    def test_envelope_without_headers_is_returned_as_is(self) -> None:
        env = MessageEnvelope(routing_key="q", body=b"x")
        prepared, _ = self._prep().prepare(0, env)
        assert prepared is env

    def test_wrong_type_is_invalid(self) -> None:
        prepared, invalid = self._prep().prepare(3, {"routing_key": "q"})
        assert prepared is None and invalid is not None
        assert invalid.status is BulkPublishStatus.INVALID
        assert invalid.reason == REASON_INVALID_TYPE
        assert invalid.index == 3
        assert isinstance(invalid.error, TypeError)

    def test_body_too_large_uses_publisher_limit(self) -> None:
        env = MessageEnvelope(routing_key="q", body=b"x" * 2000)
        _, invalid = self._prep(max_message_bytes=1024).prepare(0, env)
        assert invalid is not None and invalid.reason == REASON_BODY_TOO_LARGE
        assert isinstance(invalid.error, MessageTooLargeError)
        assert invalid.body_bytes == 2000

    def test_zero_message_limit_disables_body_check_but_not_buffer(self) -> None:
        env = MessageEnvelope(routing_key="q", body=b"x" * 5000)
        _, invalid = self._prep(max_message_bytes=0, max_buffer_bytes=4096).prepare(0, env)
        assert invalid is not None and invalid.reason == REASON_EXCEEDS_BUFFER

    def test_body_larger_than_buffer_can_never_be_admitted(self) -> None:
        env = MessageEnvelope(routing_key="q", body=b"x" * 900)
        _, invalid = self._prep(max_message_bytes=1024, max_buffer_bytes=512).prepare(0, env)
        assert invalid is not None and invalid.reason == REASON_EXCEEDS_BUFFER

    def test_invalid_header_values(self) -> None:
        env = MessageEnvelope(routing_key="q", body=b"x", headers={"x-set": {1, 2}})
        _, invalid = self._prep().prepare(0, env)
        assert invalid is not None and invalid.reason == REASON_INVALID_HEADERS

    def test_nested_header_values_ok(self) -> None:
        env = MessageEnvelope(routing_key="q", body=b"x", headers={"x": {"a": [1, "b", 2.0, None, True]}})
        prepared, invalid = self._prep().prepare(0, env)
        assert invalid is None and prepared is not None

    def test_frozen_copy_keeps_all_fields(self) -> None:
        env = MessageEnvelope(
            routing_key="q", body=b"x", headers={"h": 1}, exchange="ex", message_id="m1", priority=3, mandatory=True
        )
        prepared, _ = self._prep().prepare(0, env)
        assert prepared is not None
        assert dataclasses.asdict(prepared) == dataclasses.asdict(env)


# ── Budgets ────────────────────────────────────────────────────────────────


class TestByteBudget:
    def test_acquire_release(self) -> None:
        b = ByteBudget(100)
        assert b.acquire(60, timeout=0.1)
        assert b.used == 60
        assert not b.acquire(50, timeout=0.05)  # would exceed
        b.release(60)
        assert b.acquire(100, timeout=0.1)
        assert b.limit == 100

    def test_larger_than_limit_never_acquires(self) -> None:
        b = ByteBudget(10)
        assert not b.acquire(11, timeout=0.01)

    def test_release_unblocks_waiter(self) -> None:
        b = ByteBudget(10)
        assert b.acquire(10, timeout=0.1)
        got: list[bool] = []

        def waiter() -> None:
            got.append(b.acquire(5, timeout=2.0))

        t = threading.Thread(target=waiter)
        t.start()
        time.sleep(0.05)
        b.release(10)
        t.join(timeout=2)
        assert got == [True]

    def test_rejects_zero_limit(self) -> None:
        with pytest.raises(ValueError):
            ByteBudget(0)


class TestAsyncByteBudget:
    async def test_acquire_release(self) -> None:
        b = AsyncByteBudget(100)
        assert await b.acquire(60, timeout=0.1)
        assert b.used == 60
        assert not await b.acquire(50, timeout=0.05)
        await b.release(60)
        assert await b.acquire(100, timeout=0.1)

    async def test_release_unblocks_waiter(self) -> None:
        b = AsyncByteBudget(10)
        assert await b.acquire(10, timeout=0.1)
        task = asyncio.create_task(b.acquire(5, timeout=2.0))
        await asyncio.sleep(0.02)
        assert not task.done()
        await b.release(10)
        assert await task is True

    async def test_larger_than_limit_never_acquires(self) -> None:
        b = AsyncByteBudget(10)
        assert not await b.acquire(11, timeout=0.01)
        with pytest.raises(ValueError):
            AsyncByteBudget(0)
