"""Bulk publish runners — the shared engine behind ``publish_many`` /
``iter_publish`` on both brokers.

Transport-free (hard invariant 1): the runners are handed a ``publish_fn``
(the broker's own ``publish`` — middleware, flow control and the transport
included, so middleware runs exactly once per attempt) and turn an iterable
of envelopes into a complete stream of :class:`~rabbitkit.core.bulk.BulkPublishItem`.

Guarantees (plan §7.2):

* Every input index gets exactly one item — including items rejected
  before submission, items abandoned at the overall deadline, and items
  never reached because the caller's iterator raised.
* Admission is bounded by count (``max_in_flight``) and bytes
  (``max_buffer_bytes``); waiting is bounded by ``admission_timeout``.
  Backpressure waits or reports ``NOT_SENT`` — it never drops silently.
* Cancellation / deadline stops admission first; already-submitted items
  are classified honestly (``UNKNOWN``), never as failures.
* No automatic retry of anything.
"""

from __future__ import annotations

import asyncio
import contextlib
import time
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable, Iterable, Iterator
from typing import Any

from rabbitkit.core.bulk import (
    REASON_ADMISSION_TIMEOUT,
    REASON_CANCELLED,
    REASON_CONFIRM_TIMEOUT,
    REASON_INPUT_ERROR,
    REASON_OVERALL_TIMEOUT,
    AsyncByteBudget,
    BulkPublishItem,
    BulkPublishOptions,
    PublishPreparer,
    classify_publish_exception,
    classify_publish_outcome,
    new_attempt_id,
)
from rabbitkit.core.types import BulkPublishStatus, MessageEnvelope, PublishOutcome

SyncPublishFn = Callable[[MessageEnvelope], PublishOutcome]
AsyncPublishFn = Callable[[MessageEnvelope], Awaitable[PublishOutcome]]
ItemHook = Callable[[BulkPublishItem], None]


def _not_sent(index: int, env: MessageEnvelope, reason: str, error: BaseException | None = None) -> BulkPublishItem:
    return BulkPublishItem(
        index=index,
        status=BulkPublishStatus.NOT_SENT,
        reason=reason,
        message_id=env.message_id,
        exchange=env.exchange,
        routing_key=env.routing_key,
        body_bytes=len(env.body),
        error=error,
    )


def _from_outcome(
    index: int,
    env: MessageEnvelope,
    attempt_id: str,
    outcome: PublishOutcome | None,
    submitted_at: float,
) -> BulkPublishItem:
    status, reason = classify_publish_outcome(outcome)
    return BulkPublishItem(
        index=index,
        status=status,
        reason=reason,
        message_id=env.message_id,
        attempt_id=attempt_id,
        exchange=env.exchange,
        routing_key=env.routing_key,
        body_bytes=len(env.body),
        error=outcome.error if outcome is not None else None,
        submitted_at=submitted_at,
        settled_at=time.monotonic(),
    )


def _from_exception(
    index: int,
    env: MessageEnvelope,
    attempt_id: str,
    exc: BaseException,
    submitted_at: float,
    *,
    reason_override: str | None = None,
) -> BulkPublishItem:
    status, reason = classify_publish_exception(exc)
    if reason_override is not None:
        reason = reason_override
    return BulkPublishItem(
        index=index,
        status=status,
        reason=reason,
        message_id=env.message_id,
        attempt_id=attempt_id,
        exchange=env.exchange,
        routing_key=env.routing_key,
        body_bytes=len(env.body),
        error=exc,
        submitted_at=submitted_at,
        settled_at=time.monotonic(),
    )


# ── Sync runner ────────────────────────────────────────────────────────────


def iter_publish_sync(
    envelopes: Iterable[Any],
    publish_fn: SyncPublishFn,
    *,
    options: BulkPublishOptions,
    preparer: PublishPreparer,
    on_item: ItemHook | None = None,
) -> Iterator[BulkPublishItem]:
    """Sequentially publish *envelopes*, yielding one item per input.

    The sync transport confirms one publish at a time, so in-flight is
    always 1 and the byte budget reduces to the per-item
    ``max_buffer_bytes`` check in the preparer. ``overall_timeout`` still
    applies: once the deadline passes, every remaining input is yielded as
    ``NOT_SENT`` (``overall_timeout``) without touching the transport.

    If the caller's iterable raises mid-way, the exception propagates AFTER
    every item produced so far has been yielded — the consumer of this
    generator therefore never loses accounting for what was attempted.
    """
    deadline = time.monotonic() + options.overall_timeout if options.overall_timeout is not None else None
    iterator = iter(envelopes)
    index = 0
    while True:
        try:
            raw = next(iterator)
        except StopIteration:
            return
        except Exception:
            # Iterator failure: nothing more can be admitted; re-raise after
            # the caller has consumed everything yielded so far (generators
            # deliver exceptions at the next() that hits them, i.e. here).
            raise
        env, invalid = preparer.prepare(index, raw)
        if invalid is not None:
            if on_item is not None:
                on_item(invalid)
            yield invalid
            index += 1
            continue
        assert env is not None
        if deadline is not None and time.monotonic() >= deadline:
            item = _not_sent(index, env, REASON_OVERALL_TIMEOUT)
            if on_item is not None:
                on_item(item)
            yield item
            index += 1
            continue
        attempt_id = new_attempt_id()
        submitted_at = time.monotonic()
        try:
            outcome = publish_fn(env)
        except Exception as exc:
            item = _from_exception(index, env, attempt_id, exc, submitted_at)
        else:
            item = _from_outcome(
                index, env, attempt_id, outcome if isinstance(outcome, PublishOutcome) else None, submitted_at
            )
        if on_item is not None:
            on_item(item)
        yield item
        index += 1


# ── Async runner ───────────────────────────────────────────────────────────


async def iter_publish_async(
    envelopes: Iterable[Any] | AsyncIterator[Any],
    publish_fn: AsyncPublishFn,
    *,
    options: BulkPublishOptions,
    preparer: PublishPreparer,
    on_item: ItemHook | None = None,
) -> AsyncIterator[BulkPublishItem]:
    """Concurrently publish *envelopes* under count/byte/time bounds,
    yielding items as they complete (NOT in input order — use ``index``).

    Admission: an item waits for an in-flight slot AND a byte-budget slot,
    each bounded by ``admission_timeout`` and by the overall deadline. Items
    that cannot be admitted in time are ``NOT_SENT``.

    Deadline: when ``overall_timeout`` elapses, admission stops. Remaining
    inputs are ``NOT_SENT`` (``overall_timeout``); in-flight publishes get
    ``drain_grace`` seconds, then are cancelled and reported ``UNKNOWN``
    (``overall_timeout``) — they may have reached the broker.

    Cancellation of the consuming task behaves like the deadline: in-flight
    tasks are cancelled and reported ``UNKNOWN`` (``cancelled``) through
    ``on_item`` (the generator itself can no longer yield), then the
    ``CancelledError`` propagates.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + options.overall_timeout if options.overall_timeout is not None else None
    sem = asyncio.Semaphore(options.max_in_flight)
    budget = AsyncByteBudget(options.max_buffer_bytes)
    done_q: asyncio.Queue[BulkPublishItem] = asyncio.Queue()
    tasks: set[asyncio.Task[None]] = set()
    inflight: dict[asyncio.Task[None], tuple[int, MessageEnvelope, str, float]] = {}

    def _remaining_to_deadline() -> float | None:
        if deadline is None:
            return None
        return max(0.0, deadline - loop.time())

    async def _one(index: int, env: MessageEnvelope, attempt_id: str) -> None:
        submitted_at = time.monotonic()
        inflight[asyncio.current_task()] = (index, env, attempt_id, submitted_at)  # type: ignore[index]
        try:
            try:
                if options.confirm_timeout is not None:
                    outcome = await asyncio.wait_for(publish_fn(env), timeout=options.confirm_timeout)
                else:
                    outcome = await publish_fn(env)
            except asyncio.CancelledError:
                raise
            except TimeoutError as exc:
                item = _from_exception(
                    index, env, attempt_id, exc, submitted_at, reason_override=REASON_CONFIRM_TIMEOUT
                )
            except Exception as exc:
                item = _from_exception(index, env, attempt_id, exc, submitted_at)
            else:
                item = _from_outcome(
                    index, env, attempt_id, outcome if isinstance(outcome, PublishOutcome) else None, submitted_at
                )
            done_q.put_nowait(item)
        finally:
            await budget.release(len(env.body))
            sem.release()

    def _drain_done() -> list[BulkPublishItem]:
        out: list[BulkPublishItem] = []
        while True:
            try:
                out.append(done_q.get_nowait())
            except asyncio.QueueEmpty:
                return out

    async def _abandon_inflight(reason: str) -> list[BulkPublishItem]:
        """Cancel still-running publishes and classify them UNKNOWN."""
        items: list[BulkPublishItem] = []
        pending = [t for t in tasks if not t.done()]
        for t in pending:
            t.cancel()
        for t in pending:
            with contextlib.suppress(BaseException):
                await t
            meta = inflight.get(t)
            if meta is not None:
                index, env, attempt_id, submitted_at = meta
                items.append(
                    _from_exception(
                        index, env, attempt_id, asyncio.CancelledError(), submitted_at, reason_override=reason
                    )
                )
        return items

    def _emit(item: BulkPublishItem) -> BulkPublishItem:
        if on_item is not None:
            on_item(item)
        return item

    # Normalise sync/async input into one async iterator.
    async def _aiter() -> AsyncGenerator[Any, None]:
        if hasattr(envelopes, "__aiter__"):
            async for x in envelopes:
                yield x
        else:
            for x in envelopes:
                yield x

    index = 0
    input_error: BaseException | None = None
    deadline_hit = False
    source = _aiter()
    try:
        while True:
            try:
                raw = await source.__anext__()
            except StopAsyncIteration:
                break
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                input_error = exc
                break

            env, invalid = preparer.prepare(index, raw)
            if invalid is not None:
                yield _emit(invalid)
                index += 1
                for it in _drain_done():
                    yield _emit(it)
                continue
            assert env is not None

            if deadline_hit or (deadline is not None and loop.time() >= deadline):
                deadline_hit = True
                yield _emit(_not_sent(index, env, REASON_OVERALL_TIMEOUT))
                index += 1
                continue

            # ── admission: in-flight slot ──
            wait = options.admission_timeout
            rem = _remaining_to_deadline()
            if rem is not None:
                wait = min(wait, rem)
            try:
                await asyncio.wait_for(sem.acquire(), timeout=wait)
            except TimeoutError:
                reason = REASON_OVERALL_TIMEOUT if (rem is not None and wait == rem) else REASON_ADMISSION_TIMEOUT
                yield _emit(_not_sent(index, env, reason))
                index += 1
                for it in _drain_done():
                    yield _emit(it)
                continue
            # ── admission: byte budget ──
            wait = options.admission_timeout
            rem = _remaining_to_deadline()
            if rem is not None:
                wait = min(wait, rem)
            if not await budget.acquire(len(env.body), timeout=wait):
                sem.release()
                reason = REASON_OVERALL_TIMEOUT if (rem is not None and wait == rem) else REASON_ADMISSION_TIMEOUT
                yield _emit(_not_sent(index, env, reason))
                index += 1
                for it in _drain_done():
                    yield _emit(it)
                continue

            attempt_id = new_attempt_id()
            task = asyncio.create_task(_one(index, env, attempt_id), name=f"rabbitkit.bulk-publish-{index}")
            tasks.add(task)
            task.add_done_callback(tasks.discard)
            index += 1
            for it in _drain_done():
                yield _emit(it)

        # ── drain phase ──
        if tasks:
            grace = _remaining_to_deadline()
            timeout = None if grace is None else grace + options.drain_grace
            pending = [t for t in tasks if not t.done()]
            if pending:
                await asyncio.wait(pending, timeout=timeout)
        for it in _drain_done():
            yield _emit(it)
        if any(not t.done() for t in tasks):
            for it in await _abandon_inflight(REASON_OVERALL_TIMEOUT):
                yield _emit(it)
            for it in _drain_done():
                yield _emit(it)
        if input_error is not None:
            # Accounting is complete for everything admitted; now surface
            # the caller's own iterator failure.
            raise input_error
    except asyncio.CancelledError:
        # Consumer cancelled us: stop admitting, classify in-flight UNKNOWN
        # via on_item (we cannot yield from a cancelled generator).
        for it in await _abandon_inflight(REASON_CANCELLED):
            if on_item is not None:
                on_item(it)
        for it in _drain_done():
            if on_item is not None:
                on_item(it)
        raise
    finally:
        with contextlib.suppress(BaseException):
            await source.aclose()


__all__ = [
    "REASON_INPUT_ERROR",
    "AsyncPublishFn",
    "ItemHook",
    "SyncPublishFn",
    "iter_publish_async",
    "iter_publish_sync",
]
