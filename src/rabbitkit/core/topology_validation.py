"""Topology compatibility checks for infrastructure-managed topology (R-D8).

``TopologyMode.PASSIVE_ONLY`` means the exchanges, queues and bindings are
owned by infrastructure (Terraform, definitions.json, an operator): rabbitkit
must never write to them. A passive declare proves only that an entity
EXISTS. It cannot see a queue's type, durability or arguments, and AMQP has
no way to ask whether a binding exists — so a quorum queue declared in code
but provisioned as classic passes a passive startup unnoticed.

:func:`verify_topology` closes that gap through the management API's
read-only endpoints (``get_queue``, ``get_exchange``, ``get_queue_bindings``
— :class:`~rabbitkit.management.RabbitManagementClient` fits). Without a
management client every check it cannot make is reported ``UNVERIFIED`` with
the reason — never silently green. It performs no writes in any mode.

Transport-free (``core/`` invariant): it only calls the duck-typed client.
"""

from __future__ import annotations

import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from rabbitkit.core.topology import RabbitExchange, RabbitQueue
from rabbitkit.core.types import PreflightStatus, TopologyMode

logger = logging.getLogger(__name__)

_UNVERIFIABLE = "unverifiable without the management API (pass management_client=...)"

# Arguments compared via dedicated fields, or that only matter at creation.
_SKIP_ARGUMENTS = frozenset({"x-queue-type", "x-quorum-initial-group-size"})


@dataclass(frozen=True, slots=True)
class TopologyFinding:
    """One check: ``entity`` is ``"queue"``, ``"exchange"`` or ``"binding"``."""

    entity: str
    name: str
    check: str
    status: PreflightStatus
    detail: str = ""


@dataclass(frozen=True, slots=True)
class TopologyReport:
    """Result of :func:`verify_topology`. ``ok`` ignores ``UNVERIFIED``
    findings, which are listed separately and must be reported."""

    findings: tuple[TopologyFinding, ...] = field(default_factory=tuple)

    @property
    def failed(self) -> tuple[TopologyFinding, ...]:
        return tuple(f for f in self.findings if f.status is PreflightStatus.FAILED)

    @property
    def unverified(self) -> tuple[TopologyFinding, ...]:
        return tuple(f for f in self.findings if f.status is PreflightStatus.UNVERIFIED)

    @property
    def ok(self) -> bool:
        return not self.failed

    @property
    def fully_verified(self) -> bool:
        return self.ok and not self.unverified

    def as_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "fully_verified": self.fully_verified,
            "findings": [
                {"entity": f.entity, "name": f.name, "check": f.check, "status": f.status.value, "detail": f.detail}
                for f in self.findings
            ],
        }

    def raise_for_failures(self) -> None:
        """Raise :class:`~rabbitkit.core.errors.ConfigurationError` listing every FAILED finding."""
        if self.failed:
            from rabbitkit.core.errors import ConfigurationError

            lines = "; ".join(f"{f.entity} {f.name!r} {f.check}: {f.detail}" for f in self.failed)
            raise ConfigurationError(f"Topology is incompatible with this application's declarations: {lines}")


@dataclass(frozen=True, slots=True)
class ExpectedBinding:
    queue: str
    exchange: str
    routing_key: str
    arguments: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ExpectedTopology:
    """What an application's routes need to exist on the broker."""

    exchanges: tuple[RabbitExchange, ...] = ()
    queues: tuple[RabbitQueue, ...] = ()
    bindings: tuple[ExpectedBinding, ...] = ()
    #: Queues that need a dead-letter path (retry / terminal rejects).
    needs_dead_letter: tuple[str, ...] = ()
    #: Queues that must exist but whose definition rabbitkit does not own
    #: (delay queues and DLQs of retry-enabled routes): existence + type only.
    auxiliary: tuple[tuple[str, str], ...] = ()


def expected_topology(routes: Iterable[Any], *, broker_retry: Any = None) -> ExpectedTopology:
    """Collect the exchanges, queues and bindings *routes* rely on."""
    from rabbitkit.core.path import to_binding_key
    from rabbitkit.middleware.retry import RetryRouter

    exchanges: dict[str, RabbitExchange] = {}
    queues: dict[str, RabbitQueue] = {}
    bindings: list[ExpectedBinding] = []
    needs_dlx: list[str] = []
    auxiliary: dict[str, str] = {}
    for route in routes:
        queue: RabbitQueue = route.queue
        queues[queue.name] = queue
        exchange: RabbitExchange | None = route.exchange
        if exchange is not None and exchange.name:
            exchanges[exchange.name] = exchange
            bindings.append(
                ExpectedBinding(
                    queue=queue.name,
                    exchange=exchange.name,
                    routing_key=to_binding_key(queue.routing_key),
                    arguments=dict(queue.bind_arguments),
                )
            )
        retry = route.effective_retry_config(broker_retry)
        if retry is not None:
            needs_dlx.append(queue.name)
            router = RetryRouter(retry)
            for q in router.get_delay_queue_definitions(queue.name, "", source_queue_type=queue.queue_type):
                auxiliary[q.name] = str(q.to_declare_kwargs()["arguments"].get("x-queue-type", "classic"))
    return ExpectedTopology(
        exchanges=tuple(exchanges.values()),
        queues=tuple(queues.values()),
        bindings=tuple(bindings),
        needs_dead_letter=tuple(needs_dlx),
        auxiliary=tuple(auxiliary.items()),
    )


def _is_not_found(exc: BaseException) -> bool:
    return 404 in (getattr(exc, "code", None), getattr(exc, "status", None))


def _effective(info: dict[str, Any]) -> dict[str, Any]:
    """Declared arguments plus the effective policy (as ``x-``-prefixed keys)."""
    merged: dict[str, Any] = {}
    args = info.get("arguments") or {}
    if isinstance(args, dict):
        merged.update(args)
    policy = info.get("effective_policy_definition") or {}
    if isinstance(policy, dict):
        for key, value in policy.items():
            merged.setdefault(f"x-{key}" if not str(key).startswith("x-") else str(key), value)
    return merged


def _lookup(fn: Any, name: str, vhost: str) -> tuple[dict[str, Any] | None, TopologyFinding | None]:
    """``(info, None)`` or ``(None, finding)`` for a missing/unreadable entity."""
    try:
        info = fn(name, vhost)
    except Exception as exc:
        if _is_not_found(exc):
            return None, TopologyFinding("", name, "exists", PreflightStatus.FAILED, "does not exist")
        return None, TopologyFinding(
            "", name, "exists", PreflightStatus.UNVERIFIED, f"management lookup failed: {type(exc).__name__}"
        )
    if not isinstance(info, dict):
        return None, TopologyFinding("", name, "exists", PreflightStatus.UNVERIFIED, "unexpected management payload")
    return info, None


def _with_entity(finding: TopologyFinding, entity: str) -> TopologyFinding:
    return TopologyFinding(entity, finding.name, finding.check, finding.status, finding.detail)


def _check_queue(queue: RabbitQueue, info: dict[str, Any]) -> list[TopologyFinding]:
    out: list[TopologyFinding] = []
    name = queue.name
    actual_type = str(info.get("type") or _effective(info).get("x-queue-type") or "classic")
    expected_type = queue.queue_type.value
    out.append(
        TopologyFinding(
            "queue",
            name,
            "type",
            PreflightStatus.VERIFIED if actual_type == expected_type else PreflightStatus.FAILED,
            f"expected {expected_type}, broker has {actual_type}",
        )
    )
    if "durable" in info:
        durable = bool(info.get("durable"))
        out.append(
            TopologyFinding(
                "queue",
                name,
                "durable",
                PreflightStatus.VERIFIED if durable == queue.durable else PreflightStatus.FAILED,
                f"expected durable={queue.durable}, broker has durable={durable}",
            )
        )
    actual = _effective(info)
    for key, expected in queue.to_declare_kwargs()["arguments"].items():
        if key in _SKIP_ARGUMENTS:
            continue
        if key not in actual:
            out.append(
                TopologyFinding(
                    "queue", name, f"argument:{key}", PreflightStatus.FAILED, f"expected {expected!r}, broker has none"
                )
            )
        elif actual[key] != expected:
            out.append(
                TopologyFinding(
                    "queue",
                    name,
                    f"argument:{key}",
                    PreflightStatus.FAILED,
                    f"expected {expected!r}, broker has {actual[key]!r}",
                )
            )
        else:
            out.append(TopologyFinding("queue", name, f"argument:{key}", PreflightStatus.VERIFIED, f"{expected!r}"))
    return out


def verify_topology(
    routes: Iterable[Any],
    *,
    management_client: Any | None = None,
    vhost: str = "/",
    broker_retry: Any = None,
) -> TopologyReport:
    """Check that the broker's topology is compatible with *routes*.

    Read-only. Verifies, per route: the exchange (exists, type, durability),
    the queue (exists, type, durability, every argument the application
    declares — a broker policy that supplies the same value counts), the
    binding (source exchange + routing key), and for retry-enabled routes a
    dead-letter path on the source queue plus the existence and type of
    each delay queue and DLQ. Extra arguments the broker has are allowed.
    """
    expected = expected_topology(routes, broker_retry=broker_retry)
    findings: list[TopologyFinding] = []
    if management_client is None:
        for ex in expected.exchanges:
            findings.append(
                TopologyFinding("exchange", ex.name, "type,durable", PreflightStatus.UNVERIFIED, _UNVERIFIABLE)
            )
        for q in expected.queues:
            findings.append(
                TopologyFinding("queue", q.name, "type,durable,arguments", PreflightStatus.UNVERIFIED, _UNVERIFIABLE)
            )
        for b in expected.bindings:
            findings.append(
                TopologyFinding(
                    "binding",
                    f"{b.exchange}->{b.queue}",
                    f"routing_key={b.routing_key}",
                    PreflightStatus.UNVERIFIED,
                    _UNVERIFIABLE,
                )
            )
        for name in expected.needs_dead_letter:
            findings.append(
                TopologyFinding("queue", name, "dead-letter-path", PreflightStatus.UNVERIFIED, _UNVERIFIABLE)
            )
        return TopologyReport(findings=tuple(findings))

    for ex in expected.exchanges:
        info, missing = _lookup(management_client.get_exchange, ex.name, vhost)
        if missing is not None:
            findings.append(_with_entity(missing, "exchange"))
            continue
        assert info is not None
        actual_type = str(info.get("type", ""))
        findings.append(
            TopologyFinding(
                "exchange",
                ex.name,
                "type",
                PreflightStatus.VERIFIED if actual_type == ex.type.value else PreflightStatus.FAILED,
                f"expected {ex.type.value}, broker has {actual_type}",
            )
        )
        if "durable" in info:
            durable = bool(info["durable"])
            findings.append(
                TopologyFinding(
                    "exchange",
                    ex.name,
                    "durable",
                    PreflightStatus.VERIFIED if durable == ex.durable else PreflightStatus.FAILED,
                    f"expected durable={ex.durable}, broker has durable={durable}",
                )
            )

    queue_infos: dict[str, dict[str, Any]] = {}
    for q in expected.queues:
        info, missing = _lookup(management_client.get_queue, q.name, vhost)
        if missing is not None:
            findings.append(_with_entity(missing, "queue"))
            continue
        assert info is not None
        queue_infos[q.name] = info
        findings.extend(_check_queue(q, info))

    for name in expected.needs_dead_letter:
        info = queue_infos.get(name)
        if info is None:
            continue
        dlx = _effective(info).get("x-dead-letter-exchange")
        findings.append(
            TopologyFinding(
                "queue",
                name,
                "dead-letter-path",
                PreflightStatus.VERIFIED if dlx is not None else PreflightStatus.FAILED,
                (
                    f"dead-letter-exchange={dlx!r}"
                    if dlx is not None
                    else "retry is enabled but the queue has no dead-letter exchange (argument or policy): a "
                    "terminal reject would DISCARD the message"
                ),
            )
        )

    for name, qtype in expected.auxiliary:
        info, missing = _lookup(management_client.get_queue, name, vhost)
        if missing is not None:
            findings.append(_with_entity(missing, "queue"))
            continue
        assert info is not None
        actual_type = str(info.get("type") or "classic")
        findings.append(
            TopologyFinding(
                "queue",
                name,
                "type",
                PreflightStatus.VERIFIED if actual_type == qtype else PreflightStatus.FAILED,
                f"expected {qtype}, broker has {actual_type}",
            )
        )

    bindings_by_queue: dict[str, list[dict[str, Any]] | None] = {}
    for b in expected.bindings:
        if b.queue not in bindings_by_queue:
            try:
                raw = management_client.get_queue_bindings(b.queue, vhost)
                bindings_by_queue[b.queue] = [x for x in raw if isinstance(x, dict)] if isinstance(raw, list) else None
            except Exception:
                bindings_by_queue[b.queue] = None
        existing = bindings_by_queue[b.queue]
        label = f"{b.exchange}->{b.queue}"
        if existing is None:
            findings.append(
                TopologyFinding(
                    "binding",
                    label,
                    f"routing_key={b.routing_key}",
                    PreflightStatus.UNVERIFIED,
                    "binding lookup failed",
                )
            )
            continue
        found = any(x.get("source") == b.exchange and x.get("routing_key") == b.routing_key for x in existing)
        findings.append(
            TopologyFinding(
                "binding",
                label,
                f"routing_key={b.routing_key}",
                PreflightStatus.VERIFIED if found else PreflightStatus.FAILED,
                "present" if found else "missing (PASSIVE_ONLY never creates bindings)",
            )
        )
    return TopologyReport(findings=tuple(findings))


def log_passive_only_limits(mode: TopologyMode, routes: Sequence[Any]) -> None:
    """Startup notice for ``PASSIVE_ONLY``: what was and was not verified.

    Brokers call this after their passive declarations succeeded."""
    if mode is not TopologyMode.PASSIVE_ONLY or not routes:
        return
    logger.warning(
        "TopologyMode.PASSIVE_ONLY: rabbitkit performed no topology writes and verified only that the "
        "%d consumed queue(s) and their exchanges EXIST. Queue type, durability, arguments, dead-letter "
        "paths and bindings are unverifiable over AMQP — call broker.verify_topology(management_client=...) "
        "at startup to detect incompatibilities.",
        len(routes),
    )
