"""Stream queue consumer offset tracking.

RabbitMQ stream queues support offset-based consuming, allowing consumers
to start reading from a specific position in the log.

Offset types:
- "first": Start from the beginning of the stream
- "last": Start from the end (new messages only)
- "next": Start from the next unconsumed message (default)
- timestamp: Start from messages published after a given timestamp
- numeric offset: Start from a specific offset value

Usage::

    from rabbitkit.streams import StreamOffset, StreamConsumerConfig

    # Start from beginning
    config = StreamConsumerConfig(offset=StreamOffset.first())

    # Start from specific offset
    config = StreamConsumerConfig(offset=StreamOffset.offset(42))

    # Start from timestamp
    config = StreamConsumerConfig(offset=StreamOffset.timestamp(datetime(2026, 1, 1)))

    # Use with broker
    @broker.subscriber(
        queue=RabbitQueue("events", queue_type=QueueType.STREAM),
        stream_config=config,
    )
    def handle(body: bytes) -> None: ...
"""

from __future__ import annotations

import enum
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

logger = logging.getLogger(__name__)


class StreamOffsetType(str, enum.Enum):
    """Stream offset specification types."""

    FIRST = "first"
    LAST = "last"
    NEXT = "next"
    OFFSET = "offset"
    TIMESTAMP = "timestamp"


@dataclass(frozen=True, slots=True)
class StreamOffset:
    """Stream queue offset specification.

    Use class methods to create:
        StreamOffset.first()
        StreamOffset.last()
        StreamOffset.next()
        StreamOffset.offset(42)
        StreamOffset.timestamp(datetime(...))
    """

    type: StreamOffsetType = StreamOffsetType.NEXT
    value: int | datetime | None = None

    @classmethod
    def first(cls) -> StreamOffset:
        """Start from the beginning of the stream."""
        return cls(type=StreamOffsetType.FIRST)

    @classmethod
    def last(cls) -> StreamOffset:
        """Start from the end (new messages only)."""
        return cls(type=StreamOffsetType.LAST)

    @classmethod
    def next(cls) -> StreamOffset:
        """Start from the next unconsumed message (default)."""
        return cls(type=StreamOffsetType.NEXT)

    @classmethod
    def offset(cls, value: int) -> StreamOffset:
        """Start from a specific numeric offset."""
        if value < 0:
            msg = "Stream offset must be non-negative"
            raise ValueError(msg)
        return cls(type=StreamOffsetType.OFFSET, value=value)

    @classmethod
    def timestamp(cls, value: datetime) -> StreamOffset:
        """Start from messages published after the given timestamp."""
        return cls(type=StreamOffsetType.TIMESTAMP, value=value)

    def to_consume_arguments(self) -> dict[str, Any]:
        """Convert to RabbitMQ consume arguments (x-stream-offset).

        Returns dict suitable for merging into basic_consume arguments.
        """
        if self.type == StreamOffsetType.FIRST:
            return {"x-stream-offset": "first"}
        if self.type == StreamOffsetType.LAST:
            return {"x-stream-offset": "last"}
        if self.type == StreamOffsetType.NEXT:
            return {"x-stream-offset": "next"}
        if self.type == StreamOffsetType.OFFSET:
            return {"x-stream-offset": self.value}
        if self.type == StreamOffsetType.TIMESTAMP:
            assert isinstance(self.value, datetime)
            # RabbitMQ expects Unix timestamp in seconds
            return {"x-stream-offset": self.value}
        return {}  # pragma: no cover

    def __repr__(self) -> str:
        if self.value is not None:
            return f"StreamOffset({self.type.value}={self.value})"
        return f"StreamOffset({self.type.value})"


@dataclass(frozen=True, slots=True)
class StreamConsumerConfig:
    """Configuration for stream queue consumers.

    Extends basic consumer behavior with stream-specific options.
    """

    offset: StreamOffset = field(default_factory=StreamOffset.next)
    consumer_name: str | None = None  # x-stream-consumer-name for single-active-consumer

    def to_consume_arguments(self) -> dict[str, Any]:
        """Build consume arguments for stream queue subscription."""
        args = self.offset.to_consume_arguments()
        if self.consumer_name is not None:
            args["x-stream-consumer-name"] = self.consumer_name
        return args
