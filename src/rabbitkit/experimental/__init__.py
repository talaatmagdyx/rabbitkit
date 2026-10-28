"""rabbitkit.experimental — features under active development.

These APIs are NOT covered by the stability guarantee. They may change or
be removed in any release without a deprecation period.

Stable APIs are in the top-level ``rabbitkit`` package.

Experimental features:
- RPC (tight coupling, use with care)
- Dashboard (web UI for local development)
- Distributed locking
- Message signing
- Result backends

Stream queues are no longer experimental: import ``StreamOffset``, ``StreamOffsetType``
and ``StreamConsumerConfig`` from ``rabbitkit`` (or ``rabbitkit.streams``).
Importing them from here still works but emits a ``DeprecationWarning``; the
alias will be removed no earlier than the next minor release.

Usage::

    from rabbitkit.experimental import AsyncRPCClient, RPCClient
    from rabbitkit.experimental import create_dashboard_app
    from rabbitkit.experimental import DistributedLock, RedisLock
    from rabbitkit.experimental import SigningMiddleware
    from rabbitkit.experimental import RedisResultBackend
"""

import warnings
from typing import Any

from rabbitkit.dashboard.app import create_dashboard_app
from rabbitkit.locking import DistributedLock, LockMiddleware, RedisLock
from rabbitkit.middleware.signing import InvalidSignatureError, SigningConfig, SigningMiddleware
from rabbitkit.results.backend import RedisResultBackend, ResultBackend
from rabbitkit.results.middleware import ResultMiddleware
from rabbitkit.rpc import AsyncRPCClient, RPCClient, RPCTimeoutError

__all__ = [
    "AsyncRPCClient",
    "DistributedLock",
    "InvalidSignatureError",
    "LockMiddleware",
    "RPCClient",
    "RPCTimeoutError",
    "RedisLock",
    "RedisResultBackend",
    "ResultBackend",
    "ResultMiddleware",
    "SigningConfig",
    "SigningMiddleware",
    "create_dashboard_app",
]


#: Graduated to ``rabbitkit`` / ``rabbitkit.streams``; served here with a
#: DeprecationWarning for one minor version (docs/stability-policy.md).
_MOVED_TO_STREAMS = frozenset({"StreamConsumerConfig", "StreamOffset", "StreamOffsetType"})


def __getattr__(name: str) -> Any:
    if name in _MOVED_TO_STREAMS:
        warnings.warn(
            f"rabbitkit.experimental.{name} is deprecated: stream support is no longer experimental. "
            f"Import it from rabbitkit (or rabbitkit.streams) instead.",
            DeprecationWarning,
            stacklevel=2,
        )
        import rabbitkit.streams

        return getattr(rabbitkit.streams, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
