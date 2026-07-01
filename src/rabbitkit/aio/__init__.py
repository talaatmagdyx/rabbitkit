"""rabbitkit.aio — deprecated alias for ``rabbitkit.async_`` (L8).

``rabbitkit.async_`` is the canonical import path (it matches the actual
module structure — ``async_/broker.py``, ``async_/transport.py``, etc. — and
is what every example, doc, and test in this codebase uses). This module is
kept only so any existing ``from rabbitkit.aio import ...`` import keeps
working; importing it emits a ``DeprecationWarning``.

Prefer::

    from rabbitkit.async_.broker import AsyncBroker
    # or, once exported at top level:
    from rabbitkit import AsyncBroker
"""

import warnings

from rabbitkit.async_.broker import AsyncBroker
from rabbitkit.async_.transport import AsyncTransportImpl

warnings.warn(
    "rabbitkit.aio is deprecated -- import from rabbitkit.async_ (or rabbitkit) instead. "
    "rabbitkit.async_ is the canonical async import path.",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = [
    "AsyncBroker",
    "AsyncTransportImpl",
]
