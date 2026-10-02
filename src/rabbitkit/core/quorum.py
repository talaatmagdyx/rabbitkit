"""Quorum-queue delivery limits, and when browsing a queue is destructive.

AMQP 0-9-1 has no browse. Peeking at a queue is ``basic.get`` followed by a
requeue, and a quorum queue counts every requeue as a delivery. Once a
message passes the queue's delivery limit the broker dead-letters it, or
drops it when the queue has no dead-letter exchange. rabbitkit's DLQs have
none, so every peek of a limited quorum DLQ moves its messages one step
closer to deletion.

The semantics differ by broker version. Measured against 3.13.7 and 4.1.8:

- **4.x:** a quorum queue has a default limit of 20. The lowest non-negative
  value of the ``x-delivery-limit`` argument and the ``delivery-limit``
  policy wins, and ``-1`` means "no limit from this source".
- **3.x:** there is no default, and every configured value is a limit.
  ``-1`` is NOT unlimited there: it drops a message on its first return.

Redeclaring an existing quorum queue with a different ``x-delivery-limit``
(including adding one it was declared without) is ``406
PRECONDITION_FAILED`` on both. Fix an existing queue with a policy instead.

A quorum queue also puts every returned message at the **back** (a classic
queue puts it back where it was). So peeking at part of one rotates the
messages it read to the tail, and the next peek shows different ones. Only a
read of the whole queue, requeued in the order it was read, leaves it as it
was: see :func:`assert_whole_scan`.

Transport-free: these helpers read the management API's queue JSON and the
broker version string, nothing else.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from rabbitkit.core.errors import UnsafeToBrowseError

#: Header a quorum queue adds to every message it has delivered before. A
#: classic queue never sets it, so seeing it proves the queue is quorum.
DELIVERY_COUNT_HEADER = "x-delivery-count"

#: The limit RabbitMQ 4.x applies to a quorum queue that configures none.
DEFAULT_DELIVERY_LIMIT_4X = 20

#: How many messages a whole-queue read of a quorum queue may hold at once.
DEFAULT_MAX_QUORUM_SCAN = 5000


def broker_major_version(version: str | None) -> int | None:
    """Major version from a RabbitMQ version string (``"4.1.8"`` -> 4)."""
    if not version:
        return None
    head = str(version).strip().split(".", 1)[0]
    return int(head) if head.isdigit() else None


def dlq_delivery_limit(version: str | None) -> int | None:
    """``x-delivery-limit`` to declare a NEW quorum DLQ with, or None for none.

    ``-1`` (unlimited) on 4.x, where a quorum queue otherwise defaults to 20.
    None on 3.x, where no limit already means unlimited and ``-1`` would
    drop every message on its first return. None when the version is
    unknown, which keeps 3.x safe and leaves 4.x at its default.

    The version is the node the connection reached. In a mixed 3.x/4.x
    cluster mid-upgrade, declare new quorum DLQs only once every node runs
    4.x, or a 3.x node would treat the ``-1`` as "drop on first return".
    """
    major = broker_major_version(version)
    return -1 if major is not None and major >= 4 else None


def is_quorum(queue_info: Mapping[str, Any]) -> bool:
    arguments = queue_info.get("arguments") or {}
    return (queue_info.get("type") or arguments.get("x-queue-type")) == "quorum"


def too_deep(queue: str, messages: int, max_scan: int) -> str:
    return (
        f"{queue!r} is a quorum queue holding {messages} messages, more than one inspection reads "
        f"({max_scan}). A quorum queue puts every returned message at the back, so reading part "
        "of one would reorder it; rabbitkit reads quorum queues only whole. Raise max_quorum_scan, "
        "or shovel the queue to a classic one to inspect it."
    )


def assert_whole_scan(queue: str, queue_info: Mapping[str, Any], max_scan: int) -> None:
    """Raise :class:`UnsafeToBrowseError` if a quorum queue is too deep to read whole."""
    ready = int(queue_info.get("messages_ready", queue_info.get("messages", 0)) or 0)
    if ready > max_scan:
        raise UnsafeToBrowseError(too_deep(queue, ready, max_scan))


def effective_delivery_limit(queue_info: Mapping[str, Any], version: str | None) -> int | None:
    """Effective delivery limit of a queue, or None when requeues are free.

    *queue_info* is the management API's ``GET /api/queues/{vhost}/{name}``
    JSON. None for a classic queue, and for a quorum queue with no limit.
    """
    if not is_quorum(queue_info):
        return None
    arguments = queue_info.get("arguments") or {}
    policy = queue_info.get("effective_policy_definition")
    policy = policy if isinstance(policy, Mapping) else {}
    values = [int(v) for v in (arguments.get("x-delivery-limit"), policy.get("delivery-limit")) if v is not None]
    major = broker_major_version(version)
    if major is not None and major < 4:
        return max(0, min(values)) if values else None
    limits = [v for v in values if v >= 0]
    if limits:
        return min(limits)
    return None if values else DEFAULT_DELIVERY_LIMIT_4X


def unlimited_fix(version: str | None) -> str:
    """How to make a quorum DLQ safe to browse on this broker version."""
    major = broker_major_version(version)
    if major is not None and major < 4:
        return (
            "remove x-delivery-limit and any delivery-limit policy from it (on RabbitMQ 3.x, -1 is "
            "not unlimited: it drops a message on its first return)"
        )
    return (
        "set delivery-limit to -1 in the policy that applies to it. RabbitMQ applies only the "
        "highest-priority matching policy, so if one already matches (e.g. a rabbitkit-<queue>-dlq "
        "template), add the key there; otherwise: rabbitmqctl set_policy dlq-unlimited "
        "'\\.dlq$' '{\"delivery-limit\": -1}' --apply-to quorum_queues"
    )


def assert_browsable(queue: str, queue_info: Mapping[str, Any], version: str | None) -> None:
    """Raise :class:`UnsafeToBrowseError` if requeueing from *queue* can lose messages.

    Fails closed. An unknown broker version can't be judged. A quorum queue
    whose first statistics haven't been emitted yet (a few seconds after
    declaration) may have a policy-defined limit that isn't visible yet.
    """
    if not is_quorum(queue_info):
        return
    if broker_major_version(version) is None:
        raise UnsafeToBrowseError(
            f"{queue!r} is a quorum queue and the RabbitMQ version is unknown ({version!r}), so its "
            "delivery limit can't be judged; refusing to peek or replay it."
        )
    if "messages" not in queue_info:
        raise UnsafeToBrowseError(
            f"{queue!r} is a quorum queue whose statistics aren't available yet, so a "
            "policy-defined delivery limit can't be checked; try again in a few seconds. (If the "
            "management metrics collector is disabled they never appear: verify the limit yourself "
            "and pass check_delivery_limit=False / --no-delivery-limit-check.)"
        )
    limit = effective_delivery_limit(queue_info, version)
    if limit is not None:
        raise UnsafeToBrowseError(
            f"{queue!r} is a quorum queue with a delivery limit of {limit}. Every peek or "
            "non-matching replay counts as a delivery, and past the limit RabbitMQ drops the "
            f"message or dead-letters it away. To inspect it safely, {unlimited_fix(version)}."
        )
