"""CompressionMiddleware — envelope/body transformation.

Publish side: serialize → compress body → set content_encoding header → transport.publish
Consume side: transport delivers → check content_encoding → decompress body → deserialize
"""

from __future__ import annotations

import asyncio
import gzip
import io
import logging
import threading
import zlib
from typing import Any

from rabbitkit.core.config import CompressionConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.base import BaseMiddleware

logger = logging.getLogger(__name__)

# Bodies larger than this are decompressed in a worker thread (async path) to
# avoid blocking the event loop. The size threshold is a *secondary* trigger —
# decompression is data-dependent, so a small-on-the-wire bomb still gets
# offloaded whenever content_encoding is set.

# Streaming decompression chunk size.
_CHUNK = 64 * 1024


def _get_zstd() -> Any:
    """Lazy import of zstandard."""
    try:
        import zstandard

        return zstandard
    except ImportError:
        raise ImportError(
            "zstandard is required for zstd compression. Install it with: pip install rabbitkit[compression]"
        ) from None


class CompressionMiddleware(BaseMiddleware):
    """Envelope/body transformation for compression.

    Operates on MessageEnvelope (publish) and RabbitMessage.body (consume).
    NOT a handler-wrapping middleware — transforms data before/after serialize.

    zstd contexts are **not** thread-safe, so a ``threading.local()`` holds a
    per-thread ``ZstdCompressor``/``ZstdDecompressor`` (created lazily), giving
    concurrent workers isolated contexts without a global lock. The
    decompressed size is capped via **streaming** decompression that aborts as
    soon as the running total exceeds ``max_decompressed_size`` (zip-bomb guard).
    """

    def __init__(
        self,
        config: CompressionConfig | None = None,
        *,
        max_decompressed_size: int = 64 * 1024 * 1024,
    ) -> None:
        self._config = config or CompressionConfig()
        self._max_decompressed_size = max_decompressed_size
        # Per-thread zstd contexts (zstandard contexts are not thread-safe).
        self._zstd_local = threading.local()

    def _get_cctx(self) -> Any:
        cctx = getattr(self._zstd_local, "cctx", None)
        if cctx is None:
            zstd = _get_zstd()
            cctx = zstd.ZstdCompressor(level=self._config.level)
            self._zstd_local.cctx = cctx
        return cctx

    def _get_dctx(self) -> Any:
        dctx = getattr(self._zstd_local, "dctx", None)
        if dctx is None:
            zstd = _get_zstd()
            try:
                dctx = zstd.ZstdDecompressor(max_window_size=self._max_decompressed_size)
            except TypeError:  # older zstandard has no max_window_size
                dctx = zstd.ZstdDecompressor()
            self._zstd_local.dctx = dctx
        return dctx

    def compress(self, data: bytes) -> tuple[bytes, str | None]:
        """Compress data if above threshold.

        Returns (compressed_data, content_encoding) or (original_data, None).
        """
        if len(data) < self._config.threshold:
            return data, None

        algorithm = self._config.algorithm
        if algorithm == "gzip":
            compressed = gzip.compress(data, compresslevel=self._config.level)
            return compressed, "gzip"
        elif algorithm == "zstd":
            cctx = self._get_cctx()
            compressed = cctx.compress(data)
            return compressed, "zstd"
        else:
            raise ValueError(f"Unknown compression algorithm: {algorithm}")

    def _decompress_gzip_streaming(self, data: bytes) -> bytes:
        """Streaming gzip decompression that aborts at the size cap.

        Uses ``zlib.decompressobj(16 + MAX_WBITS)`` and feeds ``data`` through
        ``decompress(..., _CHUNK)``. Because ``max_length`` limits *output*
        (not input consumed), the unconsumed input is re-fed via
        ``unconsumed_tail`` each iteration; the running total is checked every
        chunk — a zip bomb raises before a huge allocation is materialised.
        """
        decomp = zlib.decompressobj(16 + zlib.MAX_WBITS)
        out = bytearray()
        tail: bytes = data
        while True:
            chunk = decomp.decompress(tail, _CHUNK)
            out += chunk
            if len(out) > self._max_decompressed_size:
                raise ValueError(
                    f"Decompressed size ({len(out)}) exceeds max_decompressed_size ({self._max_decompressed_size})"
                )
            tail = decomp.unconsumed_tail
            if decomp.eof:
                break
            if not chunk and not tail:
                # No output and no input left to feed — avoid spinning.
                break
        out += decomp.flush()
        if len(out) > self._max_decompressed_size:
            raise ValueError(
                f"Decompressed size ({len(out)}) exceeds max_decompressed_size ({self._max_decompressed_size})"
            )
        return bytes(out)

    def _decompress_zstd_streaming(self, data: bytes) -> bytes:
        """Streaming zstd decompression that aborts at the size cap.

        ``max_window_size`` on the decompressor rejects frames whose window
        exceeds the cap (raising ``ZstdError`` before allocating); the streaming
        ``read`` loop enforces the running-total cap for high-ratio payloads. Both
        are surfaced as ``ValueError`` so callers see a single, consistent
        zip-bomb guard.
        """
        zstd = _get_zstd()
        dctx = self._get_dctx()
        reader = dctx.stream_reader(io.BytesIO(data))
        out = bytearray()
        try:
            while True:
                chunk = reader.read(_CHUNK)
                if not chunk:
                    break
                out += chunk
                if len(out) > self._max_decompressed_size:
                    raise ValueError(
                        f"Decompressed size ({len(out)}) exceeds max_decompressed_size ({self._max_decompressed_size})"
                    )
        except zstd.ZstdError as exc:
            # Frame too large for the configured window, or other decode error —
            # treat as a zip-bomb / oversized-payload rejection.
            raise ValueError(
                f"Decompressed size exceeds max_decompressed_size ({self._max_decompressed_size})"
            ) from exc
        return bytes(out)

    def decompress(self, data: bytes, content_encoding: str | None) -> bytes:
        """Decompress data based on content_encoding header.

        Raises ``ValueError`` as soon as the running decompressed total exceeds
        ``max_decompressed_size`` (streaming zip-bomb guard).
        """
        if content_encoding is None:
            return data

        if content_encoding == "gzip":
            return self._decompress_gzip_streaming(data)
        elif content_encoding == "zstd":
            return self._decompress_zstd_streaming(data)
        else:
            logger.warning("Unknown content_encoding: %s, returning raw data", content_encoding)
            return data

    def on_receive(self, message: RabbitMessage) -> None:
        """Decompress incoming message body if content_encoding is set."""
        if message.content_encoding:
            message.body = self.decompress(message.body, message.content_encoding)

    async def on_receive_async(self, message: RabbitMessage) -> None:
        """Async variant — offloads decompression to a worker thread.

        Offload whenever there is decompression work to do (content_encoding is
        set) OR the body is large, to avoid inline-decompressing a
        small-on-the-wire bomb on the event loop.
        """
        if not message.content_encoding:
            return
        # content_encoding is set: always offload to a worker thread. A
        # small-on-the-wire zip bomb can still produce a large decompressed
        # body, so never inline-decompress on the event loop.
        message.body = await asyncio.to_thread(self.decompress, message.body, message.content_encoding)

    def transform_envelope(self, envelope: MessageEnvelope) -> MessageEnvelope:
        """Compress envelope body and set content_encoding header."""
        compressed, encoding = self.compress(envelope.body)
        if encoding is not None:
            # Create new envelope with compressed body and encoding
            headers = dict(envelope.headers)
            return MessageEnvelope(
                routing_key=envelope.routing_key,
                body=compressed,
                exchange=envelope.exchange,
                headers=headers,
                message_id=envelope.message_id,
                correlation_id=envelope.correlation_id,
                reply_to=envelope.reply_to,
                timestamp=envelope.timestamp,
                content_type=envelope.content_type,
                content_encoding=encoding,
                expiration=envelope.expiration,
                priority=envelope.priority,
                mandatory=envelope.mandatory,
                delivery_mode=envelope.delivery_mode,
                type=envelope.type,
                user_id=envelope.user_id,
                app_id=envelope.app_id,
            )
        return envelope
