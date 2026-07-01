"""rabbitkit.aio — async transport module.

This is the canonical import path for the async broker.
``rabbitkit.async_`` remains available for backwards compatibility.

Usage::

    from rabbitkit.aio import AsyncBroker
"""

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.async_.transport import AsyncTransportImpl

__all__ = [
    "AsyncBroker",
    "AsyncTransportImpl",
]
