"""DLQ triage + safe, throttled replay (docs §10/§31).

DLQInspector itself does one unthrottled pass with no dry-run; ``safe_replay``
wraps it with batching, a pause between batches, a hard cap, and an abort hook so
a replay can't turn one incident into two.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from typing import Any

from rabbitkit.core.message import RabbitMessage

logger = logging.getLogger(__name__)

Predicate = Callable[[RabbitMessage], bool]


def error_type_is(name: str) -> Predicate:
    """Replay only messages whose recorded error type matches (e.g. timeouts)."""
    return lambda m: m.headers.get("x-error-type") == name


def tenant_is(tenant_id: str) -> Predicate:
    """Replay only one tenant's messages."""
    return lambda m: m.headers.get("x-tenant-id") == tenant_id


async def safe_replay(
    inspector: Any,  # rabbitkit.dlq.DLQInspector
    dlq: str,
    predicate: Predicate,
    *,
    batch: int = 50,
    pause: float = 2.0,
    max_total: int = 5000,
    abort_check: Callable[[], bool] | None = None,
) -> int:
    """Replay matching DLQ messages in throttled batches; stop on abort/cap/empty."""
    replayed = 0
    while replayed < max_total:
        sample = await inspector.peek_async(dlq, limit=batch)
        if not sample:
            break  # DLQ drained (of currently-visible messages)
        matching = [m for m in sample if predicate(m)]
        logger.info("replay preview dlq=%s batch=%d matching=%d", dlq, len(sample), len(matching))

        replayed += await inspector.replay_async(dlq, predicate=predicate)

        if abort_check is not None and abort_check():
            logger.warning("replay aborted (error spike) dlq=%s replayed=%d", dlq, replayed)
            break
        await asyncio.sleep(pause)  # throttle to protect the downstream

    logger.info("replay done dlq=%s replayed=%d", dlq, replayed)  # audit line
    return replayed
