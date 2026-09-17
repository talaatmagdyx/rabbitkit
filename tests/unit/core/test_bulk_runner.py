"""Tests for core/bulk_runner.py — the sync/async engines behind publish_many."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator, Iterator
from typing import Any

import pytest

from rabbitkit.core.bulk import (
    REASON_ADMISSION_TIMEOUT,
    REASON_BODY_TOO_LARGE,
    REASON_CANCELLED,
    REASON_CONFIRM_TIMEOUT,
    REASON_CONFIRMED,
    REASON_INVALID_TYPE,
    REASON_NACKED,
    REASON_OVERALL_TIMEOUT,
    REASON_PUBLISH_ERROR,
    REASON_RETURNED,
    BulkPublishItem,
    BulkPublishOptions,
    PublishPreparer,
)
from rabbitkit.core.bulk_runner import iter_publish_async, iter_publish_sync
from rabbitkit.core.errors import BackpressureError
from rabbitkit.core.types import BulkPublishStatus, MessageEnvelope, PublishOutcome, PublishStatus


def _env(i: int, size: int = 1) -> MessageEnvelope:
    return MessageEnvelope(routing_key=f"q{i}", body=b"x" * size, message_id=f"id-{i}")


def _prep(max_message_bytes: int = 1024, max_buffer_bytes: int = 1 << 20) -> PublishPreparer:
    return PublishPreparer(max_message_bytes=max_message_bytes, max_buffer_bytes=max_buffer_bytes)


def _ok(env: MessageEnvelope) -> PublishOutcome:
    return PublishOutcome(status=PublishStatus.CONFIRMED, exchange=env.exchange, routing_key=env.routing_key)


# ── Sync runner ────────────────────────────────────────────────────────────


class TestIterPublishSync:
    def test_every_input_gets_exactly_one_item_in_order(self) -> None:
        envs = [_env(i) for i in range(5)]
        items = list(iter_publish_sync(envs, _ok, options=BulkPublishOptions(), preparer=_prep()))
        assert [it.index for it in items] == [0, 1, 2, 3, 4]
        assert all(it.status is BulkPublishStatus.CONFIRMED and it.reason == REASON_CONFIRMED for it in items)
        assert [it.message_id for it in items] == [f"id-{i}" for i in range(5)]
        assert all(it.attempt_id for it in items)
        assert len({it.attempt_id for it in items}) == 5
        assert all(it.duration is not None and it.duration >= 0 for it in items)

    def test_mixed_outcomes_are_mapped_per_item(self) -> None:
        outcomes = iter(
            [
                PublishOutcome(status=PublishStatus.CONFIRMED),
                PublishOutcome(status=PublishStatus.RETURNED),
                PublishOutcome(status=PublishStatus.NACKED),
                PublishOutcome(status=PublishStatus.TIMEOUT),
                PublishOutcome(status=PublishStatus.ERROR, error=BackpressureError("full")),
            ]
        )
        items = list(
            iter_publish_sync(
                [_env(i) for i in range(5)], lambda e: next(outcomes), options=BulkPublishOptions(), preparer=_prep()
            )
        )
        assert [(it.status, it.reason) for it in items] == [
            (BulkPublishStatus.CONFIRMED, REASON_CONFIRMED),
            (BulkPublishStatus.UNROUTABLE, REASON_RETURNED),
            (BulkPublishStatus.NACKED, REASON_NACKED),
            (BulkPublishStatus.UNKNOWN, REASON_CONFIRM_TIMEOUT),
            (BulkPublishStatus.NOT_SENT, "backpressure_dropped"),
        ]

    def test_invalid_items_skip_transport(self) -> None:
        calls: list[str] = []

        def pub(e: MessageEnvelope) -> PublishOutcome:
            calls.append(e.routing_key)
            return _ok(e)

        inputs: list[Any] = [_env(0), "junk", _env(2, size=5000)]
        items = list(
            iter_publish_sync(inputs, pub, options=BulkPublishOptions(), preparer=_prep(max_message_bytes=1024))
        )
        assert calls == ["q0"]
        assert items[1].status is BulkPublishStatus.INVALID and items[1].reason == REASON_INVALID_TYPE
        assert items[2].status is BulkPublishStatus.INVALID and items[2].reason == REASON_BODY_TOO_LARGE

    def test_raising_publish_is_unknown(self) -> None:
        def pub(e: MessageEnvelope) -> PublishOutcome:
            raise ConnectionResetError("socket died")

        (item,) = list(iter_publish_sync([_env(0)], pub, options=BulkPublishOptions(), preparer=_prep()))
        assert item.status is BulkPublishStatus.UNKNOWN and item.reason == REASON_PUBLISH_ERROR
        assert isinstance(item.error, ConnectionResetError)

    def test_overall_deadline_marks_remaining_not_sent(self) -> None:
        def slow(e: MessageEnvelope) -> PublishOutcome:
            time.sleep(0.03)
            return _ok(e)

        items = list(
            iter_publish_sync(
                [_env(i) for i in range(20)], slow, options=BulkPublishOptions(overall_timeout=0.05), preparer=_prep()
            )
        )
        assert len(items) == 20
        confirmed = [it for it in items if it.ok]
        not_sent = [it for it in items if it.status is BulkPublishStatus.NOT_SENT]
        assert confirmed and not_sent
        assert all(it.reason == REASON_OVERALL_TIMEOUT for it in not_sent)
        # everything after the first NOT_SENT is NOT_SENT too — admission stopped
        first = min(it.index for it in not_sent)
        assert all(it.status is BulkPublishStatus.NOT_SENT for it in items if it.index >= first)

    def test_generator_input_failure_propagates_after_yielded_items(self) -> None:
        def gen() -> Iterator[MessageEnvelope]:
            yield _env(0)
            yield _env(1)
            raise RuntimeError("db cursor died")

        seen: list[BulkPublishItem] = []
        with pytest.raises(RuntimeError, match="db cursor died"):
            for it in iter_publish_sync(gen(), _ok, options=BulkPublishOptions(), preparer=_prep()):
                seen.append(it)
        assert [it.index for it in seen] == [0, 1] and all(it.ok for it in seen)

    def test_on_item_hook_sees_every_item(self) -> None:
        seen: list[int] = []
        inputs: list[Any] = [_env(0), "junk"]
        list(
            iter_publish_sync(
                inputs, _ok, options=BulkPublishOptions(), preparer=_prep(), on_item=lambda it: seen.append(it.index)
            )
        )
        assert seen == [0, 1]

    def test_non_outcome_return_is_unknown(self) -> None:
        (item,) = list(iter_publish_sync([_env(0)], lambda e: None, options=BulkPublishOptions(), preparer=_prep()))  # type: ignore[arg-type,return-value]
        assert item.status is BulkPublishStatus.UNKNOWN and item.reason == "no_outcome"

    def test_headers_snapshot(self) -> None:
        headers = {"k": "v"}
        env = MessageEnvelope(routing_key="q", body=b"x", headers=headers)
        published: list[MessageEnvelope] = []

        def pub(e: MessageEnvelope) -> PublishOutcome:
            published.append(e)
            return _ok(e)

        list(iter_publish_sync([env], pub, options=BulkPublishOptions(), preparer=_prep()))
        headers["k"] = "mutated"
        assert published[0].headers == {"k": "v"}


# ── Async runner ───────────────────────────────────────────────────────────


async def _aok(env: MessageEnvelope) -> PublishOutcome:
    return _ok(env)


async def _collect(it: AsyncIterator[BulkPublishItem]) -> list[BulkPublishItem]:
    out: list[BulkPublishItem] = []
    async for x in it:
        out.append(x)
    return sorted(out, key=lambda i: i.index)


class TestIterPublishAsync:
    async def test_every_input_gets_exactly_one_item(self) -> None:
        items = await _collect(
            iter_publish_async(
                [_env(i) for i in range(50)], _aok, options=BulkPublishOptions(max_in_flight=8), preparer=_prep()
            )
        )
        assert [it.index for it in items] == list(range(50))
        assert all(it.ok for it in items)
        assert len({it.attempt_id for it in items}) == 50

    async def test_completion_order_may_differ_but_indices_complete(self) -> None:
        async def pub(e: MessageEnvelope) -> PublishOutcome:
            await asyncio.sleep(0.02 if e.routing_key == "q0" else 0)
            return _ok(e)

        raw: list[BulkPublishItem] = []
        async for it in iter_publish_async(
            [_env(i) for i in range(5)], pub, options=BulkPublishOptions(), preparer=_prep()
        ):
            raw.append(it)
        assert sorted(it.index for it in raw) == [0, 1, 2, 3, 4]
        assert raw[-1].index == 0  # slow one completes last

    async def test_max_in_flight_is_respected(self) -> None:
        active = 0
        peak = 0

        async def pub(e: MessageEnvelope) -> PublishOutcome:
            nonlocal active, peak
            active += 1
            peak = max(peak, active)
            await asyncio.sleep(0.005)
            active -= 1
            return _ok(e)

        await _collect(
            iter_publish_async(
                [_env(i) for i in range(40)], pub, options=BulkPublishOptions(max_in_flight=4), preparer=_prep()
            )
        )
        assert peak <= 4

    async def test_byte_budget_is_respected(self) -> None:
        inflight_bytes = 0
        peak = 0

        async def pub(e: MessageEnvelope) -> PublishOutcome:
            nonlocal inflight_bytes, peak
            inflight_bytes += len(e.body)
            peak = max(peak, inflight_bytes)
            await asyncio.sleep(0.005)
            inflight_bytes -= len(e.body)
            return _ok(e)

        opts = BulkPublishOptions(max_in_flight=100, max_buffer_bytes=250)
        await _collect(
            iter_publish_async(
                [_env(i, size=100) for i in range(10)], pub, options=opts, preparer=_prep(max_buffer_bytes=250)
            )
        )
        assert peak <= 250

    async def test_admission_timeout_reports_not_sent(self) -> None:
        release = asyncio.Event()

        async def stuck(e: MessageEnvelope) -> PublishOutcome:
            await release.wait()
            return _ok(e)

        opts = BulkPublishOptions(max_in_flight=1, admission_timeout=0.05, overall_timeout=None)
        gen = iter_publish_async([_env(0), _env(1)], stuck, options=opts, preparer=_prep())
        first = await gen.__anext__()  # item 1 cannot be admitted → NOT_SENT surfaces first
        assert first.index == 1 and first.status is BulkPublishStatus.NOT_SENT
        assert first.reason == REASON_ADMISSION_TIMEOUT
        release.set()
        rest = await _collect(gen)
        assert [it.index for it in rest] == [0] and rest[0].ok

    async def test_confirm_timeout_reports_unknown(self) -> None:
        async def slow(e: MessageEnvelope) -> PublishOutcome:
            await asyncio.sleep(0.2)
            return _ok(e)

        opts = BulkPublishOptions(confirm_timeout=0.02)
        (item,) = await _collect(iter_publish_async([_env(0)], slow, options=opts, preparer=_prep()))
        assert item.status is BulkPublishStatus.UNKNOWN and item.reason == REASON_CONFIRM_TIMEOUT

    async def test_overall_timeout_abandons_inflight_as_unknown(self) -> None:
        async def slow(e: MessageEnvelope) -> PublishOutcome:
            await asyncio.sleep(10)
            return _ok(e)

        opts = BulkPublishOptions(max_in_flight=2, overall_timeout=0.05, drain_grace=0.05, admission_timeout=5)
        items = await _collect(iter_publish_async([_env(i) for i in range(4)], slow, options=opts, preparer=_prep()))
        assert len(items) == 4
        by_status = {it.index: (it.status, it.reason) for it in items}
        assert by_status[0] == (BulkPublishStatus.UNKNOWN, REASON_OVERALL_TIMEOUT)
        assert by_status[1] == (BulkPublishStatus.UNKNOWN, REASON_OVERALL_TIMEOUT)
        assert by_status[2] == (BulkPublishStatus.NOT_SENT, REASON_OVERALL_TIMEOUT)
        assert by_status[3] == (BulkPublishStatus.NOT_SENT, REASON_OVERALL_TIMEOUT)

    async def test_raising_publish_is_unknown_and_slot_released(self) -> None:
        async def boom(e: MessageEnvelope) -> PublishOutcome:
            raise ConnectionResetError("dead")

        items = await _collect(
            iter_publish_async(
                [_env(i) for i in range(6)], boom, options=BulkPublishOptions(max_in_flight=2), preparer=_prep()
            )
        )
        assert len(items) == 6 and all(it.status is BulkPublishStatus.UNKNOWN for it in items)

    async def test_invalid_items_skip_transport(self) -> None:
        calls: list[str] = []

        async def pub(e: MessageEnvelope) -> PublishOutcome:
            calls.append(e.routing_key)
            return _ok(e)

        inputs: list[Any] = [_env(0), 42, _env(2)]
        items = await _collect(iter_publish_async(inputs, pub, options=BulkPublishOptions(), preparer=_prep()))
        assert sorted(calls) == ["q0", "q2"]
        assert items[1].status is BulkPublishStatus.INVALID

    async def test_async_iterator_input(self) -> None:
        async def agen() -> AsyncIterator[MessageEnvelope]:
            for i in range(3):
                yield _env(i)

        items = await _collect(iter_publish_async(agen(), _aok, options=BulkPublishOptions(), preparer=_prep()))
        assert [it.index for it in items] == [0, 1, 2]

    async def test_input_iterator_failure_propagates_after_accounting(self) -> None:
        def gen() -> Iterator[MessageEnvelope]:
            yield _env(0)
            raise RuntimeError("cursor died")

        seen: list[BulkPublishItem] = []
        with pytest.raises(RuntimeError, match="cursor died"):
            async for it in iter_publish_async(gen(), _aok, options=BulkPublishOptions(), preparer=_prep()):
                seen.append(it)
        assert [it.index for it in seen] == [0] and seen[0].ok

    async def test_cancellation_reports_inflight_unknown_via_hook(self) -> None:
        started = asyncio.Event()

        async def stuck(e: MessageEnvelope) -> PublishOutcome:
            started.set()
            await asyncio.sleep(10)
            return _ok(e)

        hooked: list[BulkPublishItem] = []

        async def consume() -> None:
            async for _ in iter_publish_async(
                [_env(0)],
                stuck,
                options=BulkPublishOptions(overall_timeout=None),
                preparer=_prep(),
                on_item=hooked.append,
            ):
                pass

        task = asyncio.create_task(consume())
        await started.wait()
        await asyncio.sleep(0.01)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert len(hooked) == 1
        assert hooked[0].status is BulkPublishStatus.UNKNOWN and hooked[0].reason == REASON_CANCELLED

    async def test_on_item_hook_sees_every_item(self) -> None:
        seen: list[int] = []
        inputs: list[Any] = [_env(0), "junk", _env(2)]
        await _collect(
            iter_publish_async(
                inputs, _aok, options=BulkPublishOptions(), preparer=_prep(), on_item=lambda it: seen.append(it.index)
            )
        )
        assert sorted(seen) == [0, 1, 2]

    async def test_no_overall_timeout(self) -> None:
        items = await _collect(
            iter_publish_async([_env(0)], _aok, options=BulkPublishOptions(overall_timeout=None), preparer=_prep())
        )
        assert items[0].ok
