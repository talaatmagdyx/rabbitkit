"""Property-based (hypothesis) tests (L17).

Generate a wide range of inputs rather than hand-picked examples, e.g.
serializer round-trips (``encode`` then ``decode`` recovers the original
value) across arbitrary data shapes -- the kind of edge case (unicode,
empty containers, large ints, nesting) example-based unit tests tend to
under-sample.
"""

from __future__ import annotations
