"""Tests for middleware/compression.py — CompressionMiddleware."""

from __future__ import annotations

import gzip
import threading
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, patch

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


# ── C4: publish_scope / publish_scope_async — the actual wiring point ────


class TestPublishScope:
    """C4: before this, transform_envelope() had no caller — attaching
    CompressionMiddleware to a route or broker.publish_middlewares compressed
    nothing. publish_scope/publish_scope_async are the pipeline's actual
    integration point (see core/pipeline.py compose_*_publish_sync/_async)."""

    def test_publish_scope_compresses_above_threshold(self) -> None:
        config = CompressionConfig(algorithm="gzip", threshold=10)
        mw = CompressionMiddleware(config)
        body = b"hello world " * 100
        envelope = MessageEnvelope(routing_key="rk", body=body)

        captured: list[MessageEnvelope] = []

        def call_next(env: MessageEnvelope) -> str:
            captured.append(env)
            return "outcome"

        result = mw.publish_scope(call_next, envelope)

        assert result == "outcome"
        assert len(captured) == 1
        assert captured[0].content_encoding == "gzip"
        assert gzip.decompress(captured[0].body) == body

    def test_publish_scope_passes_through_below_threshold(self) -> None:
        config = CompressionConfig(algorithm="gzip", threshold=1024)
        mw = CompressionMiddleware(config)
        envelope = MessageEnvelope(routing_key="rk", body=b"small")

        captured: list[MessageEnvelope] = []

        def call_next(env: MessageEnvelope) -> str:
            captured.append(env)
            return "outcome"

        mw.publish_scope(call_next, envelope)

        assert captured[0] is envelope  # unmodified — below threshold

    @pytest.mark.asyncio
    async def test_publish_scope_async_compresses_above_threshold(self) -> None:
        config = CompressionConfig(algorithm="gzip", threshold=10)
        mw = CompressionMiddleware(config)
        body = b"hello world " * 100
        envelope = MessageEnvelope(routing_key="rk", body=body)

        captured: list[MessageEnvelope] = []

        async def call_next(env: MessageEnvelope) -> str:
            captured.append(env)
            return "outcome"

        result = await mw.publish_scope_async(call_next, envelope)

        assert result == "outcome"
        assert len(captured) == 1
        assert captured[0].content_encoding == "gzip"
        assert gzip.decompress(captured[0].body) == body

    @pytest.mark.asyncio
    async def test_publish_scope_async_passes_through_below_threshold(self) -> None:
        config = CompressionConfig(algorithm="gzip", threshold=1024)
        mw = CompressionMiddleware(config)
        envelope = MessageEnvelope(routing_key="rk", body=b"small")

        captured: list[MessageEnvelope] = []

        async def call_next(env: MessageEnvelope) -> str:
            captured.append(env)
            return "outcome"

        await mw.publish_scope_async(call_next, envelope)

        assert captured[0] is envelope  # unmodified — below threshold


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


# ── context reuse + size cap ─────────────────────────────────────────────


class TestContextReuse:
    @pytest.fixture(autouse=True)
    def _check_zstd(self) -> None:
        pytest.importorskip("zstandard")

    def test_zstd_compressor_reused_within_thread(self) -> None:
        """The same ZstdCompressor instance is reused across compress() calls in a thread."""
        mw = CompressionMiddleware(CompressionConfig(algorithm="zstd", threshold=10, level=3))
        mw.compress(b"x" * 100)
        cctx1 = mw._get_cctx()
        mw.compress(b"y" * 100)
        cctx2 = mw._get_cctx()
        assert cctx1 is cctx2  # reused within the same thread, not recreated

    def test_zstd_decompressor_reused_within_thread(self) -> None:
        """The same ZstdDecompressor instance is reused across decompress() calls in a thread."""
        import zstandard

        mw = CompressionMiddleware(CompressionConfig(algorithm="zstd", threshold=10, level=3))
        cctx = zstandard.ZstdCompressor()
        mw.decompress(cctx.compress(b"hello world " * 10), "zstd")
        dctx1 = mw._get_dctx()
        mw.decompress(cctx.compress(b"another payload " * 10), "zstd")
        dctx2 = mw._get_dctx()
        assert dctx1 is dctx2


# ── streaming zip-bomb guard (I-2) ────────────────────────────────────────


class TestStreamingBombGuard:
    def test_gzip_bomb_raises_before_oom(self) -> None:
        """A small compressed payload that decompresses huge raises ValueError
        and never materialises the full decompressed output."""
        # 1 byte repeated ~5 MB — compresses to a few KB.
        bomb = b"\x00" * (5 * 1024 * 1024)
        compressed = gzip.compress(bomb)
        assert len(compressed) < 1024 * 1024  # small on the wire
        mw = CompressionMiddleware(max_decompressed_size=1024 * 1024)  # 1 MB cap
        with pytest.raises(ValueError, match="max_decompressed_size"):
            mw.decompress(compressed, "gzip")

    def test_gzip_within_cap_decompresses(self) -> None:
        """A normal payload within the cap decompresses correctly."""
        mw = CompressionMiddleware(max_decompressed_size=4096)
        original = b"x" * 2048
        compressed = gzip.compress(original)
        result = mw.decompress(compressed, "gzip")
        assert result == original

    def test_gzip_cap_boundary_oversized_raises(self) -> None:
        """A payload exceeding the cap by one byte raises exactly at the boundary."""
        cap = 4096
        mw = CompressionMiddleware(max_decompressed_size=cap)
        original = b"x" * (cap + 1)
        compressed = gzip.compress(original)
        with pytest.raises(ValueError, match="max_decompressed_size"):
            mw.decompress(compressed, "gzip")

    def test_gzip_cap_boundary_at_cap_decompresses(self) -> None:
        """A payload exactly at the cap decompresses."""
        cap = 4096
        mw = CompressionMiddleware(max_decompressed_size=cap)
        original = b"x" * cap
        compressed = gzip.compress(original)
        result = mw.decompress(compressed, "gzip")
        assert result == original

    def test_zstd_bomb_raises_before_oom(self) -> None:
        """zstd zip-bomb raises before materialising a huge output."""
        zstandard = pytest.importorskip("zstandard")
        bomb = b"\x00" * (5 * 1024 * 1024)
        cctx = zstandard.ZstdCompressor()
        compressed = cctx.compress(bomb)
        assert len(compressed) < 1024 * 1024
        mw = CompressionMiddleware(max_decompressed_size=1024 * 1024)
        with pytest.raises(ValueError, match="max_decompressed_size"):
            mw.decompress(compressed, "zstd")

    def test_zstd_within_cap_decompresses(self) -> None:
        zstandard = pytest.importorskip("zstandard")
        mw = CompressionMiddleware(max_decompressed_size=4096)
        original = b"x" * 2048
        compressed = zstandard.ZstdCompressor().compress(original)
        result = mw.decompress(compressed, "zstd")
        assert result == original

    @pytest.mark.asyncio
    async def test_async_gzip_bomb_raises(self) -> None:
        """A gzip bomb on the async path raises (offloaded, not on the loop)."""
        bomb = b"\x00" * (5 * 1024 * 1024)
        compressed = gzip.compress(bomb)
        mw = CompressionMiddleware(max_decompressed_size=1024 * 1024)
        msg = _make_message(body=compressed, content_encoding="gzip")
        with pytest.raises(ValueError, match="max_decompressed_size"):
            await mw.on_receive_async(msg)


# ── zstd thread-safety (I-8) ──────────────────────────────────────────────


class TestZstdThreadSafety:
    @pytest.fixture(autouse=True)
    def _check_zstd(self) -> None:
        pytest.importorskip("zstandard")

    @pytest.mark.parametrize("n_threads", [1, 2, 4, 8])
    def test_concurrent_compress_round_trips(self, n_threads: int) -> None:
        """Compressing from many threads concurrently produces outputs that all
        decompress back to the input — no corruption from shared (non-thread-safe)
        zstd contexts because each thread gets its own via threading.local()."""
        config = CompressionConfig(algorithm="zstd", threshold=10, level=3)
        mw = CompressionMiddleware(config)
        payloads = [bytes([i % 256]) * (200 + i * 17) for i in range(50)]

        results: dict[int, list[tuple[bytes, str | None]]] = {}

        def worker(tid: int) -> None:
            out: list[tuple[bytes, str | None]] = []
            for p in payloads:
                out.append(mw.compress(p))
            results[tid] = out

        with ThreadPoolExecutor(max_workers=n_threads) as pool:
            list(pool.map(worker, range(n_threads)))

        # Every compressed payload must decompress back to the original.
        import zstandard

        dctx = zstandard.ZstdDecompressor()
        for tid, compressed_list in results.items():
            assert len(compressed_list) == len(payloads)
            for (data, enc), original in zip(compressed_list, payloads, strict=True):
                assert enc == "zstd"
                assert dctx.decompress(data) == original, f"corruption from thread {tid}"

    def test_each_thread_gets_isolated_context(self) -> None:
        """Different threads get distinct ZstdCompressor instances (threading.local)."""
        mw = CompressionMiddleware(CompressionConfig(algorithm="zstd", threshold=10, level=3))
        seen: list[object] = []
        barrier = threading.Barrier(2)

        def worker() -> None:
            barrier.wait()
            seen.append(mw._get_cctx())

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join()
        t2.join()
        assert len(seen) == 2
        assert seen[0] is not seen[1]  # isolated per thread


# ── async offload behaviour ──────────────────────────────────────────────


class TestAsyncOffload:
    @pytest.mark.asyncio
    async def test_async_offloads_large_bodies(self) -> None:
        """Bodies above the offload threshold are decompressed via to_thread."""
        mw = CompressionMiddleware()
        original = b"hello world " * 5000  # > 64KB
        compressed = gzip.compress(original)
        msg = _make_message(body=compressed, content_encoding="gzip")
        await mw.on_receive_async(msg)
        assert msg.body == original

    @pytest.mark.asyncio
    async def test_async_small_encoded_body_still_correct(self) -> None:
        """A small-on-the-wire encoded body decompresses correctly (offloaded or inline)."""
        mw = CompressionMiddleware()
        original = b"hello world " * 10  # well below 64KB
        compressed = gzip.compress(original)
        msg = _make_message(body=compressed, content_encoding="gzip")
        await mw.on_receive_async(msg)
        assert msg.body == original

    @pytest.mark.asyncio
    async def test_async_no_encoding_noop(self) -> None:
        """No content_encoding → on_receive_async is a no-op."""
        mw = CompressionMiddleware()
        msg = _make_message(body=b"raw data")
        await mw.on_receive_async(msg)
        assert msg.body == b"raw data"


# ── decompression size cap (kept from prior suite) ───────────────────────


class TestDecompressionSizeCap:
    def test_oversized_gzip_decompression_raises(self) -> None:
        """Decompressing a payload above max_decompressed_size raises ValueError."""
        mw = CompressionMiddleware(max_decompressed_size=1024)
        original = b"x" * 4096
        compressed = gzip.compress(original)
        with pytest.raises(ValueError, match="max_decompressed_size"):
            mw.decompress(compressed, "gzip")

    def test_within_cap_decompresses(self) -> None:
        """Payloads at or below the cap decompress normally."""
        mw = CompressionMiddleware(max_decompressed_size=4096)
        original = b"x" * 2048
        compressed = gzip.compress(original)
        result = mw.decompress(compressed, "gzip")
        assert result == original


# ── zstd _get_dctx TypeError fallback (lines 83-84) ─────────────────────


class TestZstdDctxTypeErrorFallback:
    def test_older_zstandard_without_max_window_size(self) -> None:
        """Lines 83-84: if ZstdDecompressor raises TypeError on max_window_size,
        fall back to ZstdDecompressor() with no kwargs."""
        zstandard = pytest.importorskip("zstandard")

        mw = CompressionMiddleware()

        real_decompressor_cls = zstandard.ZstdDecompressor
        call_count = 0

        def patched_decompressor(**kwargs: object) -> object:
            nonlocal call_count
            call_count += 1
            if "max_window_size" in kwargs:
                raise TypeError("unexpected keyword argument 'max_window_size'")
            return real_decompressor_cls()

        with patch("zstandard.ZstdDecompressor", side_effect=patched_decompressor):
            dctx = mw._get_dctx()

        assert dctx is not None
        assert call_count == 2  # first with kwarg (TypeError), second without


# ── gzip streaming: empty chunk + no tail exit (lines 131) ───────────────


class TestGzipStreamingEmptyChunk:
    def test_empty_chunk_and_tail_breaks_loop(self) -> None:
        """Line 131: when decompress() returns b'' and unconsumed_tail is b''
        and eof is False, the loop breaks to avoid spinning."""

        class _StubDecomp:
            """Fake decompressobj that returns empty on first decompress, then eof."""

            def __init__(self) -> None:
                self._calls = 0
                self.eof = False
                self.unconsumed_tail: bytes = b""

            def decompress(self, data: bytes, max_length: int) -> bytes:
                self._calls += 1
                # On first call return empty with no tail and eof=False → triggers line 131
                return b""

            def flush(self) -> bytes:
                return b""

        stub = _StubDecomp()
        mw = CompressionMiddleware()
        with patch("zlib.decompressobj", return_value=stub):
            result = mw._decompress_gzip_streaming(b"dummy")

        assert result == b""

    def test_flush_over_cap_raises(self) -> None:
        """Line 134: when flush() returns data that pushes len(out) over the cap,
        ValueError is raised."""
        cap = 10

        class _StubDecompFlush:
            """Fake decompressobj where flush returns data exceeding the cap."""

            def __init__(self) -> None:
                self.eof = False
                self.unconsumed_tail: bytes = b""

            def decompress(self, data: bytes, max_length: int) -> bytes:
                # Return empty immediately so loop breaks via line 131
                return b""

            def flush(self) -> bytes:
                # Return data large enough to exceed the cap
                return b"x" * (cap + 1)

        stub = _StubDecompFlush()
        mw = CompressionMiddleware(max_decompressed_size=cap)
        with patch("zlib.decompressobj", return_value=stub):
            with pytest.raises(ValueError, match="max_decompressed_size"):
                mw._decompress_gzip_streaming(b"dummy")


# ── zstd streaming size cap + ZstdError (lines 159, 162-167) ────────────


class TestZstdStreamingZstdError:
    def test_size_cap_exceeded_raises_value_error(self) -> None:
        """Line 159: reader.read() returns data that exceeds max_decompressed_size.

        Mocks the stream reader to return a chunk larger than the cap so the
        `if len(out) > self._max_decompressed_size` branch fires at line 158-161.
        """
        pytest.importorskip("zstandard")

        cap = 10
        mw = CompressionMiddleware(max_decompressed_size=cap)

        mock_reader = MagicMock()
        # First read returns data exceeding the cap; second read would return b""
        mock_reader.read.side_effect = [b"x" * (cap + 1), b""]

        mock_dctx = MagicMock()
        mock_dctx.stream_reader.return_value = mock_reader

        with patch.object(mw, "_get_dctx", return_value=mock_dctx):
            with pytest.raises(ValueError, match="max_decompressed_size"):
                mw._decompress_zstd_streaming(b"some data")

    def test_zstd_error_during_read_raises_value_error(self) -> None:
        """Lines 162-167: ZstdError raised by stream_reader.read() is caught and
        re-raised as ValueError."""
        zstandard = pytest.importorskip("zstandard")

        mw = CompressionMiddleware()

        mock_reader = MagicMock()
        mock_reader.read.side_effect = zstandard.ZstdError("frame too large")

        mock_dctx = MagicMock()
        mock_dctx.stream_reader.return_value = mock_reader

        with patch.object(mw, "_get_dctx", return_value=mock_dctx):
            with pytest.raises(ValueError, match="max_decompressed_size"):
                mw._decompress_zstd_streaming(b"some data")
