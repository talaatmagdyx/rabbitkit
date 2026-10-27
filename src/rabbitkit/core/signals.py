"""Signal-handler ownership helpers shared by ``AsyncBroker`` and ``RabbitApp`` (G-24).

``loop.remove_signal_handler()`` resets a signal to its DEFAULT disposition,
not to what was installed before ``loop.add_signal_handler()``. Anything
that installs loop handlers temporarily must therefore capture the previous
Python-level handler (``signal.getsignal``) and put it back itself, and must
leave alone a handler another component registered through the loop (asyncio
cannot hand that one back).
"""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Callable
from typing import Any

__all__ = ["is_default_signal_handler", "is_loop_signal_handler", "loop_signal_handler_is"]


def is_loop_signal_handler(handler: Any) -> bool:
    """True if *handler* is the placeholder an event loop installs for
    ``loop.add_signal_handler`` (asyncio's and uvloop's ``_sighandler_noop``)."""
    return callable(handler) and getattr(handler, "__name__", "") == "_sighandler_noop"


def is_default_signal_handler(sig: signal.Signals, handler: Any) -> bool:
    """True for handlers nobody chose on purpose: ``SIG_DFL``, none set from
    Python, SIGINT's ``default_int_handler``, and ``asyncio.run()``'s SIGINT
    handler (a ``functools.partial`` of ``Runner._on_sigint``)."""
    if handler is None or handler == signal.SIG_DFL:
        return True
    if sig == signal.SIGINT and handler is signal.default_int_handler:
        return True
    return getattr(getattr(handler, "func", None), "__name__", "") == "_on_sigint"


def loop_signal_handler_is(loop: asyncio.AbstractEventLoop, sig: signal.Signals, callback: Callable[..., Any]) -> bool:
    """Whether *loop*'s handler for *sig* is still *callback*.

    Reads asyncio's private handler table when it exists (the standard Unix
    loop), so a handler someone else installed afterwards is not removed.
    Without it (e.g. uvloop) the handler is assumed to still be *callback*.
    """
    handlers = getattr(loop, "_signal_handlers", None)
    if not isinstance(handlers, dict):
        return True
    handle = handlers.get(sig)
    return handle is not None and getattr(handle, "_callback", None) == callback
