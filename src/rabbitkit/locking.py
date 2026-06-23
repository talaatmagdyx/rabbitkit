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
import threading
import time
import uuid
from typing import Any, Protocol, runtime_checkable

from rabbitkit.core.message import RabbitMessage
from rabbitkit.middleware.base import BaseMiddleware

_POLL_INTERVAL = 0.05  # seconds between lock-acquire retries when waiting


@runtime_checkable
class DistributedLock(Protocol):
    """Protocol for distributed lock implementations."""

    def acquire(self, key: str, timeout: float = 10.0) -> bool: ...
    def release(self, key: str) -> None: ...
    async def acquire_async(self, key: str, timeout: float = 10.0) -> bool: ...
    async def release_async(self, key: str) -> None: ...


class RedisLock:
    """Redis-based distributed lock using SET NX EX."""

    def __init__(self, redis_client: Any, prefix: str = "rabbitkit:lock:", ttl: int = 30) -> None:
        self._redis = redis_client
        self._prefix = prefix
        self._ttl = ttl
        self._lock_values: dict[str, str] = {}
        self._guard = threading.Lock()  # protects _lock_values across threads

    def _key(self, key: str) -> str:
        return f"{self._prefix}{key}"

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
            lock_value = self._lock_values.pop(key, None)
        if lock_value is not None:
            stored = self._redis.get(self._key(key))
            if stored is not None and (stored == lock_value or stored == lock_value.encode()):
                self._redis.delete(self._key(key))

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
            lock_value = self._lock_values.pop(key, None)
        if lock_value is not None:
            stored = await self._redis.get(self._key(key))
            if stored is not None and (stored == lock_value or stored == lock_value.encode()):
                await self._redis.delete(self._key(key))


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
