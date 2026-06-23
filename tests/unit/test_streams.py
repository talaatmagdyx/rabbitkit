"""Tests for stream queue consumer offset tracking."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from rabbitkit.streams import StreamConsumerConfig, StreamOffset, StreamOffsetType


class TestStreamOffsetType:
    """Tests for StreamOffsetType enum."""

    def test_first_value(self) -> None:
        assert StreamOffsetType.FIRST.value == "first"

    def test_last_value(self) -> None:
        assert StreamOffsetType.LAST.value == "last"

    def test_next_value(self) -> None:
        assert StreamOffsetType.NEXT.value == "next"

    def test_offset_value(self) -> None:
        assert StreamOffsetType.OFFSET.value == "offset"

    def test_timestamp_value(self) -> None:
        assert StreamOffsetType.TIMESTAMP.value == "timestamp"

    def test_is_str_enum(self) -> None:
        """StreamOffsetType members should be usable as plain strings."""
        assert isinstance(StreamOffsetType.FIRST, str)
        assert StreamOffsetType.FIRST == "first"

    def test_all_members(self) -> None:
        members = {m.value for m in StreamOffsetType}
        assert members == {"first", "last", "next", "offset", "timestamp"}


class TestStreamOffset:
    """Tests for StreamOffset creation and conversion."""

    def test_first(self) -> None:
        so = StreamOffset.first()
        assert so.type == StreamOffsetType.FIRST
        assert so.value is None

    def test_last(self) -> None:
        so = StreamOffset.last()
        assert so.type == StreamOffsetType.LAST
        assert so.value is None

    def test_next_default(self) -> None:
        """Default constructor produces a NEXT offset."""
        so = StreamOffset()
        assert so.type == StreamOffsetType.NEXT
        assert so.value is None

    def test_next_classmethod(self) -> None:
        so = StreamOffset.next()
        assert so.type == StreamOffsetType.NEXT
        assert so.value is None

    def test_offset_value(self) -> None:
        so = StreamOffset.offset(42)
        assert so.type == StreamOffsetType.OFFSET
        assert so.value == 42

    def test_offset_zero(self) -> None:
        """Zero is a valid offset (beginning of log)."""
        so = StreamOffset.offset(0)
        assert so.value == 0

    def test_offset_negative_raises(self) -> None:
        with pytest.raises(ValueError, match="non-negative"):
            StreamOffset.offset(-1)

    def test_timestamp(self) -> None:
        dt = datetime(2026, 1, 1, tzinfo=UTC)
        so = StreamOffset.timestamp(dt)
        assert so.type == StreamOffsetType.TIMESTAMP
        assert so.value == dt

    def test_to_consume_arguments_first(self) -> None:
        args = StreamOffset.first().to_consume_arguments()
        assert args == {"x-stream-offset": "first"}

    def test_to_consume_arguments_last(self) -> None:
        args = StreamOffset.last().to_consume_arguments()
        assert args == {"x-stream-offset": "last"}

    def test_to_consume_arguments_next(self) -> None:
        args = StreamOffset.next().to_consume_arguments()
        assert args == {"x-stream-offset": "next"}

    def test_to_consume_arguments_offset(self) -> None:
        args = StreamOffset.offset(100).to_consume_arguments()
        assert args == {"x-stream-offset": 100}

    def test_to_consume_arguments_timestamp(self) -> None:
        dt = datetime(2026, 6, 15, 12, 0, 0, tzinfo=UTC)
        args = StreamOffset.timestamp(dt).to_consume_arguments()
        assert args == {"x-stream-offset": dt}

    def test_repr_no_value(self) -> None:
        assert repr(StreamOffset.first()) == "StreamOffset(first)"
        assert repr(StreamOffset.last()) == "StreamOffset(last)"
        assert repr(StreamOffset.next()) == "StreamOffset(next)"

    def test_repr_with_value(self) -> None:
        r = repr(StreamOffset.offset(42))
        assert r == "StreamOffset(offset=42)"

    def test_repr_with_timestamp(self) -> None:
        dt = datetime(2026, 1, 1, tzinfo=UTC)
        r = repr(StreamOffset.timestamp(dt))
        assert "StreamOffset(timestamp=" in r
        assert "2026" in r

    def test_frozen(self) -> None:
        """StreamOffset is immutable."""
        so = StreamOffset.first()
        with pytest.raises(AttributeError):
            so.type = StreamOffsetType.LAST  # type: ignore[misc]

    def test_equality(self) -> None:
        """Two StreamOffsets with same type/value are equal."""
        assert StreamOffset.first() == StreamOffset.first()
        assert StreamOffset.offset(5) == StreamOffset.offset(5)
        assert StreamOffset.first() != StreamOffset.last()


class TestStreamConsumerConfig:
    """Tests for StreamConsumerConfig."""

    def test_default_offset(self) -> None:
        config = StreamConsumerConfig()
        assert config.offset.type == StreamOffsetType.NEXT
        assert config.offset.value is None

    def test_custom_offset(self) -> None:
        config = StreamConsumerConfig(offset=StreamOffset.first())
        assert config.offset.type == StreamOffsetType.FIRST

    def test_consumer_name(self) -> None:
        config = StreamConsumerConfig(consumer_name="my-consumer")
        assert config.consumer_name == "my-consumer"

    def test_consumer_name_default_none(self) -> None:
        config = StreamConsumerConfig()
        assert config.consumer_name is None

    def test_to_consume_arguments_basic(self) -> None:
        config = StreamConsumerConfig(offset=StreamOffset.first())
        args = config.to_consume_arguments()
        assert args == {"x-stream-offset": "first"}

    def test_to_consume_arguments_with_name(self) -> None:
        config = StreamConsumerConfig(
            offset=StreamOffset.last(),
            consumer_name="worker-1",
        )
        args = config.to_consume_arguments()
        assert args == {
            "x-stream-offset": "last",
            "x-stream-consumer-name": "worker-1",
        }

    def test_to_consume_arguments_offset_with_name(self) -> None:
        config = StreamConsumerConfig(
            offset=StreamOffset.offset(99),
            consumer_name="worker-2",
        )
        args = config.to_consume_arguments()
        assert args == {
            "x-stream-offset": 99,
            "x-stream-consumer-name": "worker-2",
        }

    def test_frozen(self) -> None:
        """StreamConsumerConfig is immutable."""
        config = StreamConsumerConfig()
        with pytest.raises(AttributeError):
            config.offset = StreamOffset.first()  # type: ignore[misc]
