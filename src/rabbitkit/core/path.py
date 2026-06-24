"""Named routing-key segments for ``Path()`` dependency injection.

AMQP topic wildcards (``*`` one word, ``#`` zero+ words) are anonymous, so there
is no way to bind ``Path("level")`` to a position. rabbitkit lets a route name a
single-word segment with ``{name}`` in its routing key, e.g.::

    @broker.subscriber(queue="events", routing_key="events.{level}.#")
    def handle(body: bytes, level: Annotated[str, Path("level")]) -> None: ...

``{name}`` binds to AMQP as ``*`` (one word). On each delivery the named segments
are extracted from the message's actual routing key into ``message.path``, which
is what the ``Path()`` resolver reads.
"""

from __future__ import annotations


def _is_named(segment: str) -> bool:
    return len(segment) > 2 and segment[0] == "{" and segment[-1] == "}"


def to_binding_key(routing_key: str) -> str:
    """Translate ``{name}`` segments to the AMQP single-word wildcard ``*``.

    Routing keys without named segments are returned unchanged (fast path), so
    existing topic/direct routes are completely unaffected.
    """
    if "{" not in routing_key:
        return routing_key
    return ".".join("*" if _is_named(s) else s for s in routing_key.split("."))


def extract_path(actual_routing_key: str, pattern: str) -> dict[str, str]:
    """Extract named segments from a delivered routing key given the route pattern.

    Positional: each ``{name}`` in the pattern maps to the same-index segment of
    the actual key. Stops at a ``#`` (which spans a variable number of words, so
    nothing after it has a fixed position). Returns ``{}`` when the pattern has no
    named segments — the broker already matched the binding, so no validation is
    needed here.
    """
    if "{" not in pattern:
        return {}
    actual = actual_routing_key.split(".")
    out: dict[str, str] = {}
    for i, seg in enumerate(pattern.split(".")):
        if seg == "#":
            break
        if i >= len(actual):
            break
        if _is_named(seg):
            out[seg[1:-1]] = actual[i]
    return out
