"""Tests for middleware/compression.py — CompressionMiddleware."""

from __future__ import annotations

import gzip

import pytest

from rabbitkit.core.config import CompressionConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.compression import CompressionMiddleware

# ── helpers ───────────────────────────────────────────────────────────────


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {
        "body": b"hello world",
        "routing_key": "test.rk",
    }
    defaults.update(kwargs)
    return RabbitMessage(**defaults)  # type: ignore[arg-type]


# ── compress / decompress — gzip ─────────────────────────────────────────


class TestGzipCompression:
    def test_compress_above_threshold(self) -> None:
        """Data above threshold is compressed."""
        config = CompressionConfig(algorithm="gzip", threshold=10, level=6)
        mw = CompressionMiddleware(config)

        data = b"x" * 100  # well above threshold=10
        compressed, encoding = mw.compress(data)

        assert encoding == "gzip"
        assert len(compressed) < len(data)  # gzip should reduce repeated data
        # Verify it's valid gzip
        assert gzip.decompress(compressed) == data

    def test_compress_below_threshold(self) -> None:
        """Data below threshold is NOT compressed."""
        config = CompressionConfig(algorithm="gzip", threshold=1024, level=6)
        mw = CompressionMiddleware(config)

        data = b"small"
        compressed, encoding = mw.compress(data)

        assert encoding is None
        assert compressed is data  # same object, not modified

    def test_decompress_gzip(self) -> None:
        """Gzip data is decompressed correctly."""
        mw = CompressionMiddleware()

        original = b"hello world repeated " * 50
        compressed = gzip.compress(original)

        result = mw.decompress(compressed, "gzip")
        assert result == original

    def test_decompress_none_encoding(self) -> None:
        """No content_encoding → returns data unchanged."""
        mw = CompressionMiddleware()

        data = b"raw data"
        result = mw.decompress(data, None)
        assert result is data

    def test_decompress_unknown_encoding(self) -> None:
        """Unknown content_encoding → returns raw data with warning."""
        mw = CompressionMiddleware()

        data = b"raw data"
        result = mw.decompress(data, "brotli")
        assert result is data

    def test_round_trip_gzip(self) -> None:
        """Compress + decompress round-trip preserves data."""
        config = CompressionConfig(algorithm="gzip", threshold=10, level=6)
        mw = CompressionMiddleware(config)

        original = b"hello world " * 100
        compressed, encoding = mw.compress(original)
        assert encoding == "gzip"

        decompressed = mw.decompress(compressed, encoding)
        assert decompressed == original


# ── compress / decompress — zstd ─────────────────────────────────────────


class TestZstdCompression:
    @pytest.fixture(autouse=True)
    def _check_zstd(self) -> None:
        pytest.importorskip("zstandard")

    def test_compress_zstd(self) -> None:
        """Zstd compression works when zstandard is available."""
        config = CompressionConfig(algorithm="zstd", threshold=10, level=3)
        mw = CompressionMiddleware(config)

        data = b"x" * 100
        compressed, encoding = mw.compress(data)

        assert encoding == "zstd"
        assert len(compressed) < len(data)

    def test_decompress_zstd(self) -> None:
        """Zstd decompression works."""
        import zstandard

        mw = CompressionMiddleware()

        original = b"hello world repeated " * 50
        cctx = zstandard.ZstdCompressor()
        compressed = cctx.compress(original)

        result = mw.decompress(compressed, "zstd")
        assert result == original

    def test_round_trip_zstd(self) -> None:
        """Zstd compress + decompress round-trip."""
        config = CompressionConfig(algorithm="zstd", threshold=10, level=3)
        mw = CompressionMiddleware(config)

        original = b"hello world " * 100
        compressed, encoding = mw.compress(original)
        assert encoding == "zstd"

        decompressed = mw.decompress(compressed, encoding)
        assert decompressed == original


# ── unknown algorithm ────────────────────────────────────────────────────


class TestUnknownAlgorithm:
    def test_unknown_algorithm_raises(self) -> None:
        """Unknown compression algorithm raises ValueError."""
        config = CompressionConfig(algorithm="lz4", threshold=10)
        mw = CompressionMiddleware(config)

        with pytest.raises(ValueError, match="Unknown compression algorithm"):
            mw.compress(b"x" * 100)


# ── on_receive (consume side) ────────────────────────────────────────────


class TestOnReceive:
    def test_decompresses_on_receive(self) -> None:
        """on_receive decompresses body when content_encoding is set."""
        mw = CompressionMiddleware()

        original = b"hello world " * 50
        compressed = gzip.compress(original)

        msg = _make_message(body=compressed, content_encoding="gzip")
        mw.on_receive(msg)

        assert msg.body == original

    def test_no_decompress_without_encoding(self) -> None:
        """on_receive does nothing when content_encoding is not set."""
        mw = CompressionMiddleware()

        msg = _make_message(body=b"raw data")
        mw.on_receive(msg)

        assert msg.body == b"raw data"

    @pytest.mark.asyncio
    async def test_async_on_receive(self) -> None:
        """on_receive_async decompresses just like sync."""
        mw = CompressionMiddleware()

        original = b"hello world " * 50
        compressed = gzip.compress(original)

        msg = _make_message(body=compressed, content_encoding="gzip")
        await mw.on_receive_async(msg)

        assert msg.body == original


# ── transform_envelope (publish side) ────────────────────────────────────


class TestTransformEnvelope:
    def test_compresses_envelope_above_threshold(self) -> None:
        """transform_envelope compresses body and sets content_encoding."""
        config = CompressionConfig(algorithm="gzip", threshold=10, level=6)
        mw = CompressionMiddleware(config)

        body = b"hello world " * 100
        envelope = MessageEnvelope(
            routing_key="test.rk",
            body=body,
            exchange="test",
        )

        result = mw.transform_envelope(envelope)

        assert result is not envelope  # new envelope
        assert result.content_encoding == "gzip"
        assert gzip.decompress(result.body) == body
        # Other fields preserved
        assert result.routing_key == "test.rk"
        assert result.exchange == "test"

    def test_no_compress_below_threshold(self) -> None:
        """transform_envelope returns same envelope below threshold."""
        config = CompressionConfig(algorithm="gzip", threshold=1024)
        mw = CompressionMiddleware(config)

        body = b"small"
        envelope = MessageEnvelope(routing_key="rk", body=body)

        result = mw.transform_envelope(envelope)

        assert result is envelope  # same object

    def test_preserves_all_envelope_fields(self) -> None:
        """All MessageEnvelope fields are preserved after compression."""
        config = CompressionConfig(algorithm="gzip", threshold=10)
        mw = CompressionMiddleware(config)

        envelope = MessageEnvelope(
            routing_key="test.rk",
            body=b"x" * 100,
            exchange="my-exchange",
            headers={"x-custom": "value"},
            message_id="msg-123",
            correlation_id="corr-456",
            reply_to="reply-queue",
            content_type="application/json",
            mandatory=True,
            delivery_mode=2,
            type="order.created",
            app_id="my-app",
        )

        result = mw.transform_envelope(envelope)

        assert result.routing_key == "test.rk"
        assert result.exchange == "my-exchange"
        assert result.message_id == "msg-123"
        assert result.correlation_id == "corr-456"
        assert result.reply_to == "reply-queue"
        assert result.content_type == "application/json"
        assert result.content_encoding == "gzip"
        assert result.mandatory is True
        assert result.delivery_mode == 2
        assert result.type == "order.created"
        assert result.app_id == "my-app"


# ── default config ───────────────────────────────────────────────────────


class TestDefaultConfig:
    def test_default_config(self) -> None:
        """Default config uses gzip, threshold=1024, level=6."""
        mw = CompressionMiddleware()
        assert mw._config.algorithm == "gzip"
        assert mw._config.threshold == 1024
        assert mw._config.level == 6

    def test_custom_config(self) -> None:
        """Custom config overrides defaults."""
        config = CompressionConfig(algorithm="zstd", threshold=512, level=3)
        mw = CompressionMiddleware(config)
        assert mw._config.algorithm == "zstd"
        assert mw._config.threshold == 512
        assert mw._config.level == 3


class TestGetZstdMissing:
    def test_raises_import_error_when_zstandard_missing(self) -> None:
        """Lines 27-28: ImportError raised when zstandard is not installed."""
        import sys
        from unittest.mock import patch

        from rabbitkit.middleware.compression import _get_zstd

        with patch.dict(sys.modules, {"zstandard": None}):
            with pytest.raises(ImportError, match="zstandard is required"):
                _get_zstd()
