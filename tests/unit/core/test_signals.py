"""Tests for core/signals.py — signal-handler ownership helpers (G-24)."""

from __future__ import annotations

import asyncio
import functools
import signal
from typing import Any
from unittest.mock import MagicMock

from rabbitkit.core.signals import is_default_signal_handler, is_loop_signal_handler, loop_signal_handler_is


def test_loop_placeholder_is_recognised() -> None:
    def _sighandler_noop(signum: int, frame: Any) -> None:
        pass

    assert is_loop_signal_handler(_sighandler_noop)
    assert not is_loop_signal_handler(signal.SIG_DFL)
    assert not is_loop_signal_handler(lambda s, f: None)


def test_default_handlers() -> None:
    class Runner:
        def _on_sigint(self, signum: int, frame: Any, main_task: Any) -> None:
            pass

    assert is_default_signal_handler(signal.SIGTERM, None)
    assert is_default_signal_handler(signal.SIGTERM, signal.SIG_DFL)
    assert is_default_signal_handler(signal.SIGINT, signal.default_int_handler)
    assert is_default_signal_handler(signal.SIGINT, functools.partial(Runner()._on_sigint, main_task=None))
    assert not is_default_signal_handler(signal.SIGTERM, signal.default_int_handler)
    assert not is_default_signal_handler(signal.SIGTERM, lambda s, f: None)


async def test_loop_handler_ownership() -> None:
    loop = asyncio.get_running_loop()
    saved = signal.getsignal(signal.SIGTERM)

    def mine() -> None:
        pass

    try:
        assert not loop_signal_handler_is(loop, signal.SIGTERM, mine)
        loop.add_signal_handler(signal.SIGTERM, mine)
        assert loop_signal_handler_is(loop, signal.SIGTERM, mine)
        assert not loop_signal_handler_is(loop, signal.SIGTERM, lambda: None)
    finally:
        loop.remove_signal_handler(signal.SIGTERM)
        signal.signal(signal.SIGTERM, saved)


def test_loop_without_a_handler_table_is_assumed_to_match() -> None:
    assert loop_signal_handler_is(MagicMock(spec=[]), signal.SIGTERM, lambda: None)
