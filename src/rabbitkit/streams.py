"""RabbitMQ stream queues: start offsets, offset stores, consumer config.

Consume a stream through a normal route; rabbitkit sets ``x-stream-offset``,
tracks each delivery's offset, resumes after a reconnect from the last
processed message, and commits the position to an :class:`OffsetStore` so a
restart resumes too::

    from rabbitkit import QueueType, RabbitQueue, StreamConsumerConfig

    @broker.subscriber(
        RabbitQueue("events", queue_type=QueueType.STREAM, max_age="7D"),
        # offset_store: your own OffsetStore (load/save over your storage) to
        # resume across restarts; the default in-memory store covers reconnects.
        stream=StreamConsumerConfig(offset="first", offset_store=my_offset_store),
    )
    async def handle(body: dict, message: RabbitMessage) -> None:
        print(message.stream_offset, body)

    # shorthand when the default in-memory store is enough
    @broker.subscriber(RabbitQueue("audit", queue_type=QueueType.STREAM), stream_offset="first")
    async def audit(body: bytes) -> None: ...

See ``docs/streams.md`` for the delivery semantics (at-least-once, no
retry/DLQ on stream routes, the commit window) and when to choose a stream
over a quorum queue. The implementation lives in ``rabbitkit.core.streams``.
"""

from __future__ import annotations

from rabbitkit.core.streams import (
    InMemoryOffsetStore,
    OffsetStore,
    StreamConsumerConfig,
    StreamOffset,
    StreamOffsetLike,
)
from rabbitkit.core.types import StreamOffsetType

__all__ = [
    "InMemoryOffsetStore",
    "OffsetStore",
    "StreamConsumerConfig",
    "StreamOffset",
    "StreamOffsetLike",
    "StreamOffsetType",
]
