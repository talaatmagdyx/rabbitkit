"""Security regression tests (L17).

Black-box scenarios (public middleware API only -- publish_scope/on_receive,
never internal helpers like ``_compute_fresh_signature``) for the attack
classes rabbitkit explicitly defends against: signing replay and
decompression zip-bombs. Unit tests for the same middlewares
(``tests/unit/middleware/test_signing.py``, ``test_compression.py``) already
cover the implementation in detail; these tests exist separately, under a
name a security reviewer will actually find, and stay valid across internal
refactors because they only use the public surface.
"""

from __future__ import annotations
