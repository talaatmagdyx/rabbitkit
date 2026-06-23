"""CompressionMiddleware — envelope/body transformation.

Publish side: serialize → compress body → set content_encoding header → transport.publish
Consume side: transport delivers → check content_encoding → decompress body → deserialize
"""

from __future__ import annotations

import gzip
import logging
from typing import Any

from rabbitkit.core.config import CompressionConfig
from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.types import MessageEnvelope
from rabbitkit.middleware.base import BaseMiddleware

logger = logging.getLogger(__name__)


def _get_zstd() -> Any:
    """Lazy import of zstandard."""
    try:
        import zstandard

        return zstandard
    except ImportError:
        raise ImportError(
            "zstandard is required for zstd compression. "
            "Install it with: pip install rabbitkit[compression]"
        ) from None


class CompressionMiddleware(BaseMiddleware):
    """Envelope/body transformation for compression.

    Operates on MessageEnvelope (publish) and RabbitMessage.body (consume).
    NOT a handler-wrapping middleware — transforms data before/after serialize.
    """

    def __init__(self, config: CompressionConfig | None = None) -> None:
        self._config = config or CompressionConfig()

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
            zstd = _get_zstd()
            cctx = zstd.ZstdCompressor(level=self._config.level)
            compressed = cctx.compress(data)
            return compressed, "zstd"
        else:
            raise ValueError(f"Unknown compression algorithm: {algorithm}")

    def decompress(self, data: bytes, content_encoding: str | None) -> bytes:
        """Decompress data based on content_encoding header."""
        if content_encoding is None:
            return data

        if content_encoding == "gzip":
            return gzip.decompress(data)
        elif content_encoding == "zstd":
            zstd = _get_zstd()
            dctx = zstd.ZstdDecompressor()
            return dctx.decompress(data)  # type: ignore[no-any-return]
        else:
            logger.warning("Unknown content_encoding: %s, returning raw data", content_encoding)
            return data

    def on_receive(self, message: RabbitMessage) -> None:
        """Decompress incoming message body if content_encoding is set."""
        if message.content_encoding:
            message.body = self.decompress(message.body, message.content_encoding)

    async def on_receive_async(self, message: RabbitMessage) -> None:
        """Async variant — same logic, compression is CPU-bound."""
        self.on_receive(message)

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
