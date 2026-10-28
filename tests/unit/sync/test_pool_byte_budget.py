"""sync/pool.py — InFlightByteLimiter, the sync ``max_in_flight_bytes`` budget."""

from __future__ import annotations

import threading
import time
from collections.abc import Callable

import pytest

from rabbitkit.sync.pool import InFlightByteLimiter


def _until(predicate: Callable[[], bool], timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not predicate():
        assert time.monotonic() < deadline, "condition not reached"
        time.sleep(0.001)


def _waiting(limiter: InFlightByteLimiter, n: int) -> None:
    _until(lambda: len(limiter._waiters) == n)


class _Taker:
    """``acquire`` on its own thread; the result lands in ``result``."""

    def __init__(
        self,
        limiter: InFlightByteLimiter,
        n: int,
        timeout: float = 5.0,
        order: list[str] | None = None,
        name: str = "",
    ) -> None:
        self.result: list[bool] = []
        self.error: list[BaseException] = []

        def run() -> None:
            try:
                ok = limiter.acquire(n, timeout)
            except BaseException as exc:
                self.error.append(exc)
                return
            if order is not None:
                order.append(name)
            self.result.append(ok)

        self.thread = threading.Thread(target=run, daemon=True)
        self.thread.start()

    def join(self) -> bool:
        self.thread.join(timeout=5)
        assert not self.thread.is_alive()
        return self.result[0]


class TestBasics:
    def test_rejects_a_non_positive_limit(self) -> None:
        with pytest.raises(ValueError, match=">= 1"):
            InFlightByteLimiter(0)

    def test_fast_path_and_release(self) -> None:
        limiter = InFlightByteLimiter(10)
        assert limiter.acquire(4, timeout=0.1)
        assert (limiter.used, limiter.limit) == (4, 10)
        limiter.release(4)
        assert limiter.used == 0

    def test_larger_than_the_limit_is_refused_at_once(self) -> None:
        started = time.monotonic()
        assert not InFlightByteLimiter(3).acquire(4, timeout=10.0)
        assert time.monotonic() - started < 1.0

    def test_release_never_goes_below_zero(self) -> None:
        limiter = InFlightByteLimiter(5)
        limiter.release(3)
        assert limiter.used == 0


class TestWaiting:
    def test_waits_then_times_out_and_leaves_no_waiter(self) -> None:
        limiter = InFlightByteLimiter(5)
        assert limiter.acquire(5, timeout=0.1)
        assert not limiter.acquire(1, timeout=0.02)
        assert limiter.used == 5
        assert not limiter._waiters

    def test_a_release_admits_a_blocked_thread(self) -> None:
        limiter = InFlightByteLimiter(5)
        assert limiter.acquire(5, timeout=0.1)
        taker = _Taker(limiter, 3)
        _waiting(limiter, 1)
        limiter.release(5)
        assert taker.join()
        assert limiter.used == 3

    def test_waiters_are_granted_in_fifo_order(self) -> None:
        limiter = InFlightByteLimiter(5)
        assert limiter.acquire(3, timeout=0.1)
        order: list[str] = []
        big = _Taker(limiter, 5, order=order, name="big")
        _waiting(limiter, 1)
        small = _Taker(limiter, 2, order=order, name="small")
        _waiting(limiter, 2)
        assert limiter.used == 3  # small fits now, but must not overtake big
        limiter.release(3)
        assert big.join()
        assert limiter.used == 5 and len(limiter._waiters) == 1
        limiter.release(5)
        assert small.join()
        assert order == ["big", "small"]
        assert limiter.used == 2

    def test_a_timed_out_head_unblocks_the_queue(self) -> None:
        limiter = InFlightByteLimiter(5)
        assert limiter.acquire(3, timeout=0.1)
        head = _Taker(limiter, 5, timeout=0.05)  # cannot fit while 3 are held
        _waiting(limiter, 1)
        behind = _Taker(limiter, 2, timeout=5.0)  # fits, but queued behind head
        assert head.join() is False
        assert behind.join()
        assert limiter.used == 5
        assert not limiter._waiters

    def test_concurrent_acquires_never_exceed_the_limit(self) -> None:
        limiter = InFlightByteLimiter(10)
        peak: list[int] = []
        peak_lock = threading.Lock()
        failures: list[int] = []

        def worker(n: int) -> None:
            for _ in range(50):
                if not limiter.acquire(n, timeout=5.0):
                    failures.append(n)
                    continue
                with peak_lock:
                    peak.append(limiter.used)
                limiter.release(n)

        threads = [threading.Thread(target=worker, args=(n,)) for n in (1, 3, 4, 7, 10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=30)
        assert not failures
        assert max(peak) <= 10
        assert limiter.used == 0 and not limiter._waiters


class TestPump:
    def test_a_pumping_waiter_pumps_instead_of_sleeping_until_granted(self) -> None:
        limiter = InFlightByteLimiter(5)
        assert limiter.acquire(5, timeout=0.1)
        slices: list[float] = []

        def pump(seconds: float) -> bool:
            slices.append(seconds)
            if len(slices) == 3:  # the "I/O loop" runs the holder's publish
                limiter.release(5)
            return True

        assert limiter.acquire(4, timeout=5.0, pump=pump)
        assert len(slices) == 3
        assert all(0 < s <= 0.01 for s in slices)
        assert limiter.used == 4

    def test_a_pump_that_cannot_pump_waits_on_the_condition(self) -> None:
        limiter = InFlightByteLimiter(5)
        assert limiter.acquire(5, timeout=0.1)
        calls: list[float] = []

        def pump(seconds: float) -> bool:
            calls.append(seconds)
            if len(calls) == 2:
                threading.Thread(target=limiter.release, args=(5,)).start()
            return False

        assert limiter.acquire(2, timeout=5.0, pump=pump)
        assert len(calls) >= 2
        assert limiter.used == 2

    def test_a_pumping_waiter_times_out(self) -> None:
        limiter = InFlightByteLimiter(5)
        assert limiter.acquire(5, timeout=0.1)
        assert not limiter.acquire(1, timeout=0.03, pump=lambda _s: True)
        assert limiter.used == 5 and not limiter._waiters

    def test_a_pump_error_withdraws_the_waiter_and_unblocks_the_queue(self) -> None:
        limiter = InFlightByteLimiter(5)
        assert limiter.acquire(3, timeout=0.1)
        go = threading.Event()
        behind_result: list[bool] = []

        def boom(_seconds: float) -> bool:
            # Let a smaller request queue behind us, then fail.
            go.set()
            _waiting(limiter, 2)
            raise ConnectionResetError("gone")

        def behind() -> None:
            go.wait(5)
            behind_result.append(limiter.acquire(2, timeout=5.0))

        t = threading.Thread(target=behind)
        t.start()
        with pytest.raises(ConnectionResetError):
            limiter.acquire(5, timeout=5.0, pump=boom)
        t.join(timeout=5)
        assert behind_result == [True]
        assert limiter.used == 5 and not limiter._waiters

    def test_bytes_granted_as_the_pump_fails_are_handed_back(self) -> None:
        limiter = InFlightByteLimiter(5)
        assert limiter.acquire(5, timeout=0.1)

        def grant_then_fail(_seconds: float) -> bool:
            limiter.release(5)  # grants our waiter ...
            raise KeyboardInterrupt  # ... and the wait still fails

        with pytest.raises(KeyboardInterrupt):
            limiter.acquire(4, timeout=5.0, pump=grant_then_fail)
        assert limiter.used == 0 and not limiter._waiters
