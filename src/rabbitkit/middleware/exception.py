"""ExceptionMiddleware — outermost middleware for exception handling.

Catches exceptions AFTER retry gives up.
Provides fallback values for error recovery.

See Contract 1 for terminal vs non-terminal behavior.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from typing import Any

from rabbitkit.core.message import RabbitMessage
from rabbitkit.middleware.base import BaseMiddleware

logger = logging.getLogger(__name__)


class ExceptionMiddleware(BaseMiddleware):
    """Outermost middleware. Catches exceptions after retry gives up.

    Features:
    - Register exception handlers with fallback values
    - Terminal exceptions (from retry exhaustion) are re-raised by default
    - swallow_permanent=True opts in to swallowing permanent/exhausted failures

    MANUAL mode restriction:
    - MAY log the error
    - MUST NOT auto-publish fallback to result_publisher unless msg.is_settled
    - MUST NOT settle the message
    """

    def __init__(self, *, swallow_permanent: bool = False) -> None:
        self._handlers: dict[type[BaseException], Callable[[BaseException], Any]] = {}
        self._swallow_permanent = swallow_permanent

    def add_handler(
        self,
        exc_type: type[BaseException],
        handler: Callable[[BaseException], Any],
    ) -> None:
        """Register an exception handler with a fallback return value."""
        self._handlers[exc_type] = handler

    def consume_scope(
        self,
        call_next: Callable[[RabbitMessage], Any],
        message: RabbitMessage,
    ) -> Any:
        """Wrap handler — catch exceptions, provide fallback values."""
        try:
            return call_next(message)
        except Exception as exc:
            return self._handle_exception(exc, message)

    async def consume_scope_async(
        self,
        call_next: Callable[[RabbitMessage], Awaitable[Any]],
        message: RabbitMessage,
    ) -> Any:
        """Async variant — catch exceptions, provide fallback values."""
        try:
            return await call_next(message)
        except Exception as exc:
            return self._handle_exception(exc, message)

    def _handle_exception(self, exc: Exception, message: RabbitMessage) -> Any:
        """Handle an exception with registered handlers or re-raise.

        Terminal exceptions (tagged with _rabbitkit_terminal=True by RetryMiddleware)
        are only swallowed if swallow_permanent=True.
        """
        is_terminal = getattr(exc, "_rabbitkit_terminal", False)

        if is_terminal and not self._swallow_permanent:
            logger.error(
                "Terminal exception (permanent/exhausted): %s: %s",
                type(exc).__name__,
                exc,
            )
            raise

        # Try registered handlers
        for exc_type, handler in self._handlers.items():
            if isinstance(exc, exc_type):
                logger.warning(
                    "Exception handled by %s handler: %s",
                    exc_type.__name__,
                    exc,
                )
                return handler(exc)

        # No handler found
        if is_terminal and self._swallow_permanent:
            logger.warning(
                "Swallowing terminal exception (swallow_permanent=True): %s: %s",
                type(exc).__name__,
                exc,
            )
            return None

        # Re-raise unhandled non-terminal exceptions
        raise
