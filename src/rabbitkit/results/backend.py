"""Result backend protocol and implementations.

A **result backend** stores handler return values so callers can retrieve
them later using a ``correlation_id``.  This is the server side of the
request/result pattern — the broker stores results; clients fetch them.

Protocol
--------
``ResultBackend`` is a ``@runtime_checkable`` generic ``Protocol[T]``.  ``T``
is the type of the stored result, so the type flows from
``store(correlation_id, result: T)`` into ``fetch(correlation_id) -> T | None``.
Any object with ``store`` / ``fetch`` (sync) and ``store_async`` / ``fetch_async``
(async) methods qualifies.

Built-in implementation
-----------------------
``RedisResultBackend`` stores results as Redis strings with configurable TTL,
so it satisfies ``ResultBackend[bytes]``:

    import redis
    from rabbitkit.results.backend import RedisResultBackend

    r = redis.Redis(host="redis")
    backend = RedisResultBackend(r, key_prefix="myapp:result:")

Keys are stored as ``{key_prefix}{correlation_id}``.  Default TTL is 3600 s (1 h).

Async variant (redis-py >= 4.2 async client)::

    import redis.asyncio as aioredis
    r = aioredis.Redis(host="redis")
    backend = RedisResultBackend(r)
    await backend.store_async("corr-123", b'{"status": "done"}', ttl=600)
    result = await backend.fetch_async("corr-123")

Custom backend
--------------
Implement the protocol to use any other storage:

    class PostgresResultBackend:
        def store(self, correlation_id: str, result: bytes, ttl: int = 3600) -> None:
            db.execute("INSERT INTO results ...", (correlation_id, result))

        def fetch(self, correlation_id: str, timeout: float = 5.0) -> bytes | None:
            row = db.fetchone("SELECT result FROM results WHERE id=%s", (correlation_id,))
            return row[0] if row else None

        async def store_async(...): ...
        async def fetch_async(...): ...

See also
--------
``ResultMiddleware`` — middleware that wires this backend to the pipeline so
handler return values are stored automatically.
"""

from __future__ import annotations

from typing import Any, Protocol, TypeVar, runtime_checkable

T = TypeVar("T")


@runtime_checkable
class ResultBackend(Protocol[T]):
    """Protocol for result storage backends.

    Generic in ``T`` (the stored result type) so the type flows from
    ``store`` into ``fetch``.  ``RedisResultBackend`` stores ``bytes``, so it
    satisfies ``ResultBackend[bytes]``.
    """

    def store(self, correlation_id: str, result: T, ttl: int = 3600) -> None: ...
    def fetch(self, correlation_id: str, timeout: float = 5.0) -> T | None: ...
    async def store_async(self, correlation_id: str, result: T, ttl: int = 3600) -> None: ...
    async def fetch_async(self, correlation_id: str, timeout: float = 5.0) -> T | None: ...


class RedisResultBackend:
    """Redis-based result backend using GET/SET with TTL.

    Stores results as raw bytes, so it satisfies ``ResultBackend[bytes]``.
    """

    def __init__(self, redis_client: Any, key_prefix: str = "rabbitkit:result:") -> None:
        self._redis = redis_client
        self._prefix = key_prefix

    def _key(self, correlation_id: str) -> str:
        return f"{self._prefix}{correlation_id}"

    def store(self, correlation_id: str, result: bytes, ttl: int = 3600) -> None:
        self._redis.set(self._key(correlation_id), result, ex=ttl)

    def fetch(self, correlation_id: str, timeout: float = 5.0) -> bytes | None:
        return self._redis.get(self._key(correlation_id))  # type: ignore[no-any-return]

    async def store_async(self, correlation_id: str, result: bytes, ttl: int = 3600) -> None:
        await self._redis.set(self._key(correlation_id), result, ex=ttl)

    async def fetch_async(self, correlation_id: str, timeout: float = 5.0) -> bytes | None:
        return await self._redis.get(self._key(correlation_id))  # type: ignore[no-any-return]
