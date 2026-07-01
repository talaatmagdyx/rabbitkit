"""Security regression: CompressionMiddleware decompression bombs (L17).

Black-box scenario using only the public middleware API (``on_receive``, the
actual per-message consume hook): an attacker publishes a message with
``content_encoding`` set and a tiny on-the-wire body engineered to expand to
an enormous decompressed size (a "zip bomb" / "decompression bomb"). The
middleware must reject it via the streaming size cap before materializing
the full decompressed output, not eventually run the process out of memory.

See ``tests/unit/middleware/test_compression.py``'s "streaming zip-bomb
guard (I-2)" class for implementation-level coverage of the same
streaming-abort mechanics.
"""

from __future__ import annotations

import gzip

import pytest

from rabbitkit.core.message import RabbitMessage
from rabbitkit.middleware.compression import CompressionMiddleware

# A real zip bomb: highly-compressible repeated bytes at a size chosen to
# demonstrate genuine amplification (>1000x), not just "a big file".
_BOMB_DECOMPRESSED_SIZE = 500 * 1024 * 1024  # 500 MiB
_CAP = 1 * 1024 * 1024  # 1 MiB -- comfortably smaller than the bomb


def _make_message(*, body: bytes, content_encoding: str) -> RabbitMessage:
    return RabbitMessage(body=body, routing_key="attacker.controlled", content_encoding=content_encoding)


class TestGzipDecompressionBomb:
    def test_high_ratio_gzip_bomb_is_rejected(self) -> None:
        """A gzip payload compressing >1000x is rejected before the full
        output is materialized."""
        bomb_wire_bytes = gzip.compress(b"\x00" * _BOMB_DECOMPRESSED_SIZE)
        # Verify this really is a bomb (huge amplification), not an
        # accidentally-weak test fixture.
        amplification = _BOMB_DECOMPRESSED_SIZE / len(bomb_wire_bytes)
        assert amplification > 1000, f"fixture is not actually a bomb (ratio={amplification:.0f}x)"

        mw = CompressionMiddleware(max_decompressed_size=_CAP)
        message = _make_message(body=bomb_wire_bytes, content_encoding="gzip")

        with pytest.raises(ValueError, match="max_decompressed_size"):
            mw.on_receive(message)

    def test_legitimate_payload_within_cap_still_decompresses(self) -> None:
        """Sanity check: the guard doesn't reject ordinary compressed
        traffic that stays within the configured cap."""
        payload = b"ordinary message body" * 10
        wire_bytes = gzip.compress(payload)

        mw = CompressionMiddleware(max_decompressed_size=_CAP)
        message = _make_message(body=wire_bytes, content_encoding="gzip")

        mw.on_receive(message)  # must not raise
        assert message.body == payload


class TestZstdDecompressionBomb:
    @pytest.fixture(autouse=True)
    def _check_zstd(self) -> None:
        pytest.importorskip("zstandard")

    def test_high_ratio_zstd_bomb_is_rejected(self) -> None:
        """A zstd payload compressing >1000x is rejected before the full
        output is materialized."""
        import zstandard as zstd

        cctx = zstd.ZstdCompressor()
        bomb_wire_bytes = cctx.compress(b"\x00" * _BOMB_DECOMPRESSED_SIZE)
        amplification = _BOMB_DECOMPRESSED_SIZE / len(bomb_wire_bytes)
        assert amplification > 1000, f"fixture is not actually a bomb (ratio={amplification:.0f}x)"

        mw = CompressionMiddleware(max_decompressed_size=_CAP)
        message = _make_message(body=bomb_wire_bytes, content_encoding="zstd")

        with pytest.raises(ValueError, match="max_decompressed_size"):
            mw.on_receive(message)

    def test_legitimate_payload_within_cap_still_decompresses(self) -> None:
        import zstandard as zstd

        payload = b"ordinary message body" * 10
        cctx = zstd.ZstdCompressor()
        wire_bytes = cctx.compress(payload)

        mw = CompressionMiddleware(max_decompressed_size=_CAP)
        message = _make_message(body=wire_bytes, content_encoding="zstd")

        mw.on_receive(message)  # must not raise
        assert message.body == payload
