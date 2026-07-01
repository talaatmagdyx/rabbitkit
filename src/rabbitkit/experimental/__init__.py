"""rabbitkit.experimental — features under active development.

These APIs are NOT covered by the stability guarantee. They may change or
be removed in any release without a deprecation period.

Stable APIs are in the top-level ``rabbitkit`` package.

Experimental features:
- RPC (tight coupling, use with care)
- Dashboard (web UI for local development)
- Stream queues
- Distributed locking
- Message signing
- Result backends

Usage::

    from rabbitkit.experimental import AsyncRPCClient, RPCClient
    from rabbitkit.experimental import create_dashboard_app
    from rabbitkit.experimental import DistributedLock, RedisLock
    from rabbitkit.experimental import SigningMiddleware
    from rabbitkit.experimental import RedisResultBackend
"""

from rabbitkit.dashboard.app import create_dashboard_app
from rabbitkit.locking import DistributedLock, LockMiddleware, RedisLock
from rabbitkit.middleware.signing import InvalidSignatureError, SigningConfig, SigningMiddleware
from rabbitkit.results.backend import RedisResultBackend, ResultBackend
from rabbitkit.results.middleware import ResultMiddleware
from rabbitkit.rpc import AsyncRPCClient, RPCClient, RPCTimeoutError
from rabbitkit.streams import StreamConsumerConfig, StreamOffset, StreamOffsetType

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
    "StreamConsumerConfig",
    "StreamOffset",
    "StreamOffsetType",
    "create_dashboard_app",
]
