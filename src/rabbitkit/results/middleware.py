"""Result storage middleware — stores handler return values.

``ResultMiddleware`` intercepts handler return values and persists them to a
``ResultBackend`` (e.g. ``RedisResultBackend``) keyed by ``correlation_id``.

This enables the **fire-and-retrieve** pattern:

1. Publisher sends a message with a ``correlation_id``.
2. Consumer handler returns a result.
3. ``ResultMiddleware`` stores ``result`` at ``backend[correlation_id]``.
4. Publisher polls / waits for ``backend.fetch(correlation_id)``.

Quick start
-----------
    import redis
    from rabbitkit.results.backend import RedisResultBackend
    from rabbitkit.results.middleware import ResultMiddleware

    r = redis.Redis()
    backend = RedisResultBackend(r, key_prefix="orders:result:")
    result_mw = ResultMiddleware(backend, ttl=300)

    @broker.subscriber(queue="orders", middlewares=[result_mw])
    def handle_order(body: bytes) -> bytes:
        # This return value is automatically stored in Redis
        return b'{"processed": true}'

Retrieving from the caller side::

    import time
    result = None
    for _ in range(50):        # poll for up to 5 s
        result = backend.fetch(correlation_id, timeout=0.1)
        if result is not None:
            break
        time.sleep(0.1)

Custom serializer
-----------------
Pass a serializer to control how non-bytes return values are encoded:

    from rabbitkit.serialization.json import JsonSerializer

    result_mw = ResultMiddleware(backend, serializer=JsonSerializer(), ttl=600)

    @broker.subscriber(queue="calc", middlewares=[result_mw])
    def compute(body: bytes) -> dict:
        return {"answer": 42}    # encoded with JsonSerializer before storage

Messages without ``correlation_id``
-------------------------------------
If the incoming message has no ``correlation_id``, the result is silently
discarded (no exception raised).
"""

from __future__ import annotations

import json
from typing import Any

from rabbitkit.core.message import RabbitMessage
from rabbitkit.middleware.base import BaseMiddleware
from rabbitkit.results.backend import ResultBackend


class ResultMiddleware(BaseMiddleware):
    """Stores handler return values in a result backend.

    Keyed by the message's correlation_id. If no correlation_id, skips storage.
    """

    def __init__(self, backend: ResultBackend[Any], serializer: Any | None = None, ttl: int = 3600) -> None:
        self._backend = backend
        self._serializer = serializer
        self._ttl = ttl

    def _serialize(self, result: Any) -> bytes:
        if isinstance(result, bytes):
            return result
        if self._serializer is not None and hasattr(self._serializer, "encode"):
            return self._serializer.encode(result)  # type: ignore[no-any-return]
        return json.dumps(result, default=str).encode("utf-8")

    def consume_scope(self, call_next: Any, message: RabbitMessage) -> Any:
        result = call_next(message)
        if result is not None and message.correlation_id:
            self._backend.store(message.correlation_id, self._serialize(result), self._ttl)
        return result

    async def consume_scope_async(self, call_next: Any, message: RabbitMessage) -> Any:
        result = await call_next(message)
        if result is not None and message.correlation_id:
            await self._backend.store_async(message.correlation_id, self._serialize(result), self._ttl)
        return result
