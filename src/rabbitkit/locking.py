"""Distributed locking — protocol + Redis implementation + middleware.

Ensures that only **one consumer across the entire cluster** processes a
message for a given key at a time.  Useful for preventing duplicate processing
when multiple instances consume the same queue.

Architecture
------------
``DistributedLock`` — ``@runtime_checkable`` protocol any lock implementation
must satisfy.  Provides both sync (``acquire`` / ``release``) and async
(``acquire_async`` / ``release_async``) variants.

``RedisLock`` — reference implementation using Redis ``SET NX EX``:
* Generates a per-acquisition UUID as the lock value.
* ``release()`` fetches the stored value first and only deletes if it still
  matches, preventing another holder's lock from being deleted by a stale
  release call.

``LockMiddleware`` — ``BaseMiddleware`` that acquires a lock before invoking
the handler and releases it in a ``finally`` block.

Quick start
-----------
    import redis
    from rabbitkit.locking import RedisLock, LockMiddleware

    r = redis.Redis(host="redis", decode_responses=False)
    lock = RedisLock(r, prefix="myapp:lock:", ttl=30)
    lock_mw = LockMiddleware(lock, timeout=5.0)

    @broker.subscriber(queue="orders", middlewares=[lock_mw])
    async def handle_order(body: bytes) -> None:
        # Guaranteed: only one instance handles the same routing_key at once
        ...

Custom lock key
---------------
By default the lock key is ``message.routing_key``.  Supply ``key_fn`` for
finer-grained control:

    # Lock per order ID extracted from body (JSON)
    import json

    lock_mw = LockMiddleware(
        lock,
        key_fn=lambda msg: json.loads(msg.body)["order_id"],
        timeout=10.0,
    )

When the lock cannot be acquired
---------------------------------
The message is nacked with ``requeue=True`` so another consumer or a later
retry attempt can process it.  With a retry topology this becomes a natural
wait-and-retry loop without busy-polling.

Bring your own lock
-------------------
Any object satisfying the ``DistributedLock`` protocol works:

    class ZooKeeperLock:
        def acquire(self, key: str, timeout: float = 10.0) -> bool: ...
        def release(self, key: str) -> None: ...
        async def acquire_async(self, key: str, timeout: float = 10.0) -> bool: ...
        async def release_async(self, key: str) -> None: ...

    lock_mw = LockMiddleware(ZooKeeperLock(...))
"""

from __future__ import annotations

import asyncio
import logging
import threading
import time
import uuid
from typing import Any, Protocol, runtime_checkable

from rabbitkit.core.message import RabbitMessage
from rabbitkit.middleware.base import BaseMiddleware

logger = logging.getLogger(__name__)

_POLL_INTERVAL = 0.05  # seconds between lock-acquire retries when waiting


@runtime_checkable
class DistributedLock(Protocol):
    """Protocol for distributed lock implementations."""

    def acquire(self, key: str, timeout: float = 10.0) -> bool: ...
    def release(self, key: str) -> None: ...
    async def acquire_async(self, key: str, timeout: float = 10.0) -> bool: ...
    async def release_async(self, key: str) -> None: ...


class RedisLock:
    """Redis-based distributed lock using SET NX EX.

    Release uses an atomic Lua compare-and-delete so a stale holder can never
    delete another owner's lock. The per-acquisition UUID is also exposed as a
    *fencing token* via :attr:`fencing_token` for use in downstream writes that
    need to guard against reordered operations.

    L3 — ``ttl`` has no auto-renewal: if a handler runs longer than ``ttl``,
    the lock expires while the handler is still working, and a second
    consumer can acquire the same key and start processing concurrently —
    the exact condition this lock exists to prevent. There is no watchdog
    here that periodically extends the TTL. Set ``ttl`` comfortably above
    your worst-case handler time (including retries/timeouts on the handler
    side), and for any downstream write that must not be applied twice even
    under a lost lock, use :meth:`fencing_token` — pass it along with the
    write and have the downstream store reject a token older than the one it
    already recorded, so a "lock expired, second holder also wrote" race is
    caught at the write itself rather than relying on the lock alone.
    """

    # Atomic compare-and-delete: only deletes the key if the stored value
    # matches the caller's lock value. Returns 1 on delete, 0 otherwise.
    _RELEASE_SCRIPT = (
        "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end"
    )
    _RELEASE_SCRIPT_ASYNC = _RELEASE_SCRIPT

    def __init__(self, redis_client: Any, prefix: str = "rabbitkit:lock:", ttl: int = 30) -> None:
        self._redis = redis_client
        self._prefix = prefix
        self._ttl = ttl
        self._lock_values: dict[str, str] = {}
        self._guard = threading.Lock()  # protects _lock_values across threads
        # SHA1 digest of the loaded release script (cached after first eval).
        self._release_sha: str | None = None

    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"

    def fencing_token(self, key: str) -> str | None:
        """Return the lock value (UUID) for *key* as a fencing token.

        ``None`` if this lock does not currently hold *key*.
        """
        with self._guard:
            return self._lock_values.get(key)

    def acquire(self, key: str, timeout: float = 10.0) -> bool:
        """Acquire the lock. Waits up to ``timeout`` seconds (polling); ``timeout
        <= 0`` makes a single non-blocking attempt."""
        lock_value = uuid.uuid4().hex
        redis_key = self._key(key)
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            if self._redis.set(redis_key, lock_value, nx=True, ex=self._ttl):
                with self._guard:
                    self._lock_values[key] = lock_value
                return True
            if timeout <= 0 or time.monotonic() >= deadline:
                return False
            time.sleep(_POLL_INTERVAL)

    def release(self, key: str) -> None:
        with self._guard:
            lock_value = self._lock_values.get(key)
        if lock_value is None:
            return
        deleted = self._eval_release(self._key(key), lock_value)
        # L-5: only drop local tracking after a successful delete. On a transport
        # error the value stays tracked so the TTL (or a later retry) cleans up,
        # rather than silently stranding the lock and losing the fencing token.
        if deleted:
            with self._guard:
                # Re-check the value hasn't been replaced by a re-acquire in the
                # meantime before popping.
                if self._lock_values.get(key) == lock_value:
                    self._lock_values.pop(key, None)

    def _eval_release(self, redis_key: str, lock_value: str) -> bool:
        """Atomically delete the lock only if its stored value matches.

        Returns True when the lock was deleted, False when the script returned 0
        (stale/foreign lock) or the client lacks ``eval``. A real transport error
        is logged and reported as "not deleted" so the caller keeps the local
        tracking intact (L-5) rather than silently swallowing it.
        """
        # Prefer EVALSHA with the cached SHA, falling back to EVAL. Many test
        # doubles only implement `eval`, so keep `eval` as the primary path and
        # treat any AttributeError / failure as "use eval".
        try:
            result = self._redis.eval(self._RELEASE_SCRIPT, 1, redis_key, lock_value)
        except AttributeError:
            # No eval support at all — nothing we can do safely.
            return False
        except Exception as exc:
            # Some clients raise when the script returns 0 (no delete); that's
            # expected for a stale/foreign lock. A real transport error, however,
            # must not be silently swallowed — log it so operators notice, and
            # signal "not deleted" so the local tracking is preserved (L-5).
            logger.warning("Redis EVAL failed during lock release: %s", exc)
            return False
        return bool(result)

    async def acquire_async(self, key: str, timeout: float = 10.0) -> bool:
        """Async variant of :meth:`acquire` (polls with ``asyncio.sleep``)."""
        lock_value = uuid.uuid4().hex
        redis_key = self._key(key)
        deadline = time.monotonic() + max(0.0, timeout)
        while True:
            if await self._redis.set(redis_key, lock_value, nx=True, ex=self._ttl):
                with self._guard:
                    self._lock_values[key] = lock_value
                return True
            if timeout <= 0 or time.monotonic() >= deadline:
                return False
            await asyncio.sleep(_POLL_INTERVAL)

    async def release_async(self, key: str) -> None:
        with self._guard:
            lock_value = self._lock_values.get(key)
        if lock_value is None:
            return
        deleted = await self._eval_release_async(self._key(key), lock_value)
        # L-5: only drop local tracking after a successful delete.
        if deleted:
            with self._guard:
                if self._lock_values.get(key) == lock_value:
                    self._lock_values.pop(key, None)

    async def _eval_release_async(self, redis_key: str, lock_value: str) -> bool:
        """Async variant of :meth:`_eval_release`."""
        try:
            result = await self._redis.eval(self._RELEASE_SCRIPT_ASYNC, 1, redis_key, lock_value)
        except AttributeError:
            return False
        except Exception as exc:
            logger.warning("Redis EVAL failed during async lock release: %s", exc)
            return False
        return bool(result)


class LockMiddleware(BaseMiddleware):
    """Acquire a lock before processing, release after.

    If lock cannot be acquired, nacks message with requeue=True.
    Default key_fn uses routing_key.
    """

    def __init__(
        self,
        lock: DistributedLock,
        key_fn: Any | None = None,
        timeout: float = 0.0,
    ) -> None:
        # Default 0.0 = non-blocking: on contention, nack(requeue=True) immediately
        # rather than blocking the consumer. Set timeout > 0 to wait for the lock.
        self._lock = lock
        self._key_fn = key_fn or (lambda m: m.routing_key)
        self._timeout = timeout

    def consume_scope(self, call_next: Any, message: RabbitMessage) -> Any:
        key = self._key_fn(message)
        if not self._lock.acquire(key, self._timeout):
            if not message.is_settled:
                message.nack(requeue=True)
            return None
        try:
            return call_next(message)
        finally:
            self._lock.release(key)

    async def consume_scope_async(self, call_next: Any, message: RabbitMessage) -> Any:
        key = self._key_fn(message)
        if not await self._lock.acquire_async(key, self._timeout):
            if not message.is_settled:
                await message.nack_async(requeue=True)
            return None
        try:
            return await call_next(message)
        finally:
            await self._lock.release_async(key)
