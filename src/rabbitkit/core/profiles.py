"""Reliability profiles, preflight verification, and broker policy templates.

Two explicit profiles (plan §9; ADR-M1/M4 of the messaging contract):

==========================  ========================  ==============================================
Setting                     STANDARD                  CRITICAL
==========================  ========================  ==============================================
Confirms                    on                        required
Mandatory routing           on                        required
Persistence                 on                        required
Body limit                  explicit                  <= 256 KiB
In-flight publish bytes     explicit                  bounded (default 64 MiB, overridable)
Reserved headers            pass through              stripped from application publishes
Queue type                  explicit choice           quorum for every durable route queue
Source dead-lettering       broker default            at-least-once + reject-publish (declared or
                                                      by policy; verified by :func:`preflight`)
Retry                       explicit                  required on every route: quorum delay chain
                                                      (at-least-once) + quorum DLQ, terminal DLQ
                                                      copy with metadata, retry count from
                                                      ``x-death`` only (``trust_retry_header=False``)
Transient failure w/o retry  bounded (2 strikes)      bounded (``reject_transient_on_redelivery``)
MANUAL handler raises       AUTO error path           AUTO error path (``manual_settle_on_error``)
Processing deadline         explicit                  required (``consumer.processing_timeout``)
``AckPolicy.ACK_FIRST``     allowed                   rejected on every route
Error text                  sanitized                 sanitized or omitted (never raw)
Dead-letter path            auto-provisioned          never ``discard``
Event metadata validation   opt-in                    opt-in (an application contract choice)
==========================  ========================  ==============================================

Profiles are opt-in. :func:`apply_profile` returns a NEW ``RabbitConfig``
with the profile's requirements filled in and raises on a *contradiction*
(a setting the caller pinned explicitly to a value the profile forbids) —
it never silently flips a value you set on purpose. A requirement that has
no safe default (CRITICAL's ``consumer.processing_timeout``) raises too.
:func:`validate_profile` just reports, including per-route requirements
(queue type, ack policy, per-route retry) that a config alone cannot carry.

A library cannot see effective broker policies over AMQP. :func:`preflight`
therefore verifies what it can locally, verifies the rest through a
read-only management-API client when one is supplied, and otherwise reports
each broker-side requirement as ``UNVERIFIED`` — never as green.

:func:`policy_templates` renders reviewed RabbitMQ policy definitions (the
management-API JSON shape) for at-least-once dead-lettering, reject-publish
overflow and delivery limits. They are for review and deliberate
application by whoever owns the cluster; rabbitkit never applies them.
"""

from __future__ import annotations

import dataclasses
from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

from rabbitkit.core.config import (
    ConsumerConfig,
    PublisherConfig,
    RabbitConfig,
    RetryConfig,
    RetryDisabled,
    SafetyConfig,
)
from rabbitkit.core.errors import ConfigValidationError
from rabbitkit.core.types import (
    AckPolicy,
    DeadLetterStrategy,
    PreflightStatus,
    QueueType,
    ReliabilityProfile,
    TerminalAction,
)

#: Team baseline body limit for the critical profile (256 KiB).
CRITICAL_MAX_MESSAGE_BYTES = 256 * 1024

#: Default in-flight publish byte budget the critical profile sets when the
#: config has none (``PublisherConfig.max_in_flight_bytes``, async broker):
#: 64 MiB, i.e. 256 messages at the 256 KiB body limit. Set your own value to
#: override it.
CRITICAL_MAX_IN_FLIGHT_BYTES = 64 * 1024 * 1024

_PROCESSING_TIMEOUT_REQUIRED = (
    "consumer.processing_timeout is required by the CRITICAL profile and has no safe default: set "
    "ConsumerConfig(processing_timeout=<seconds>) to the longest a handler may legitimately run. On expiry "
    "the handler is cancelled and the delivery is retried as UNKNOWN_OUTCOME (handlers must be idempotent). "
    "Async broker only; on SyncBroker use TimeoutMiddleware"
)


@dataclass(frozen=True, slots=True)
class ProfileViolation:
    """One profile requirement the configuration does not meet."""

    setting: str
    expected: str
    actual: str
    severity: str = "error"  # "error" | "warning"

    def __str__(self) -> str:
        return f"{self.setting}: expected {self.expected}, got {self.actual} ({self.severity})"


# ── Validation ─────────────────────────────────────────────────────────────


def _route_queues(routes: Sequence[Any]) -> list[Any]:
    """Best-effort extraction of ``RabbitQueue`` objects from route definitions."""
    queues: list[Any] = []
    for r in routes:
        q = getattr(r, "queue", None)
        if q is not None:
            queues.append(q)
    return queues


def _critical_retry_violations(prefix: str, retry: RetryConfig) -> list[ProfileViolation]:
    """CRITICAL requirements on one ``RetryConfig`` (the broker's or a route's)."""
    out: list[ProfileViolation] = []
    if retry.delay_queue_type != "quorum":
        out.append(ProfileViolation(f"{prefix}.delay_queue_type", "'quorum'", repr(retry.delay_queue_type)))
    if retry.dlq_queue_type not in ("quorum", "inherit"):
        out.append(ProfileViolation(f"{prefix}.dlq_queue_type", "'quorum' or 'inherit'", repr(retry.dlq_queue_type)))
    if retry.delay_queue_dead_letter_strategy != DeadLetterStrategy.AT_LEAST_ONCE.value:
        out.append(
            ProfileViolation(
                f"{prefix}.delay_queue_dead_letter_strategy",
                "'at-least-once'",
                repr(retry.delay_queue_dead_letter_strategy),
            )
        )
    if retry.terminal_action is not TerminalAction.DEAD_LETTER:
        out.append(
            ProfileViolation(
                f"{prefix}.terminal_action",
                "'dead_letter' (confirmed DLQ copy with failure metadata)",
                repr(retry.terminal_action.value),
            )
        )
    if retry.trust_retry_header:
        out.append(ProfileViolation(f"{prefix}.trust_retry_header", "False (count retries from x-death only)", "True"))
    if retry.error_detail == "raw":
        out.append(ProfileViolation(f"{prefix}.error_detail", "'sanitized' or 'omit'", "'raw'"))
    return out


def _declares_at_least_once(queue: Any) -> bool:
    if getattr(queue, "dead_letter_strategy", None) is DeadLetterStrategy.AT_LEAST_ONCE:
        return True
    args = getattr(queue, "arguments", None) or {}
    return bool(args.get("x-dead-letter-strategy") == DeadLetterStrategy.AT_LEAST_ONCE.value)


def _critical_route_violations(config: RabbitConfig, routes: Sequence[Any]) -> list[ProfileViolation]:
    out: list[ProfileViolation] = []
    retry = config.retry
    for r in routes:
        rname = str(getattr(r, "name", "?"))
        if getattr(r, "ack_policy", None) is AckPolicy.ACK_FIRST:
            out.append(
                ProfileViolation(f"route[{rname}].ack_policy", "not ACK_FIRST (ack only after success)", "ACK_FIRST")
            )
        override = getattr(r, "retry_override", None)
        if isinstance(override, RetryDisabled):
            out.append(ProfileViolation(f"route[{rname}].retry", "retry enabled (RetryConfig)", "RETRY_DISABLED"))
        elif isinstance(override, RetryConfig):
            out.extend(_critical_retry_violations(f"route[{rname}].retry", override))
    for q in _route_queues(routes):
        qname = getattr(q, "name", "?")
        qtype = getattr(q, "queue_type", None)
        durable = getattr(q, "durable", True)
        if durable and qtype is not QueueType.QUORUM:
            out.append(
                ProfileViolation(
                    f"queue[{qname}].queue_type",
                    "QueueType.QUORUM",
                    str(getattr(qtype, "value", qtype)),
                )
            )
        if retry is not None and retry.dlq_queue_type == "inherit" and qtype is not QueueType.QUORUM:
            out.append(
                ProfileViolation(
                    f"queue[{qname}].dlq",
                    "quorum DLQ (source quorum or dlq_queue_type='quorum')",
                    "classic (inherited)",
                )
            )
        if qtype is QueueType.QUORUM and not _declares_at_least_once(q):
            # A broker policy may provide it (policy_templates()); only the
            # management API can tell, so this is a warning locally and a
            # broker check in preflight().
            out.append(
                ProfileViolation(
                    f"queue[{qname}].dead_letter_strategy",
                    "'at-least-once' + overflow 'reject-publish' (declared, or by policy: verify with "
                    "preflight(management_client=...))",
                    "not declared",
                    severity="warning",
                )
            )
    return out


def validate_profile(
    config: RabbitConfig,
    profile: ReliabilityProfile | str,
    *,
    routes: Sequence[Any] = (),
) -> list[ProfileViolation]:
    """Return every requirement of *profile* that *config* (and *routes*) violate.

    Empty list == compliant. Local checks only — see :func:`preflight` for
    broker-side verification.
    """
    prof = ReliabilityProfile(profile)
    out: list[ProfileViolation] = []
    pub: PublisherConfig = config.publisher
    safety: SafetyConfig = config.safety
    retry: RetryConfig | None = config.retry

    # Shared (standard + critical): the guardrails both profiles turn on.
    if not pub.confirm_delivery:
        out.append(ProfileViolation("publisher.confirm_delivery", "True", "False"))
    if not pub.persistent:
        out.append(ProfileViolation("publisher.persistent", "True", "False"))
    if not pub.mandatory:
        out.append(
            ProfileViolation(
                "publisher.mandatory",
                "True",
                "False",
                severity="error" if prof is ReliabilityProfile.CRITICAL else "warning",
            )
        )
    if pub.max_message_bytes == 0:
        out.append(ProfileViolation("publisher.max_message_bytes", "> 0 (bounded)", "0 (unbounded)"))
    if safety.reject_without_dlx == "discard":
        out.append(ProfileViolation("safety.reject_without_dlx", "'auto_provision' or 'error'", "'discard'"))
    if retry is not None and retry.error_detail == "raw" and prof is ReliabilityProfile.STANDARD:
        out.append(ProfileViolation("retry.error_detail", "'sanitized' or 'omit'", "'raw'"))

    if prof is ReliabilityProfile.CRITICAL:
        out.extend(_critical_config_violations(config))
        out.extend(_critical_route_violations(config, routes))
    return out


def _critical_config_violations(config: RabbitConfig) -> list[ProfileViolation]:
    out: list[ProfileViolation] = []
    pub = config.publisher
    consumer: ConsumerConfig = config.consumer
    if pub.max_message_bytes > CRITICAL_MAX_MESSAGE_BYTES:
        out.append(
            ProfileViolation(
                "publisher.max_message_bytes",
                f"<= {CRITICAL_MAX_MESSAGE_BYTES}",
                str(pub.max_message_bytes),
            )
        )
    if pub.max_in_flight_bytes is None:
        out.append(ProfileViolation("publisher.max_in_flight_bytes", "a byte bound", "None (unbounded)"))
    if not pub.strip_reserved_headers:
        out.append(ProfileViolation("publisher.strip_reserved_headers", "True", "False"))
    if config.retry is None:
        out.append(ProfileViolation("retry", "RetryConfig(...) (bounded retry + DLQ)", "None"))
    else:
        out.extend(_critical_retry_violations("retry", config.retry))
    if consumer.processing_timeout is None:
        out.append(ProfileViolation("consumer.processing_timeout", _PROCESSING_TIMEOUT_REQUIRED, "None"))
    if not consumer.reject_transient_on_redelivery:
        out.append(ProfileViolation("consumer.reject_transient_on_redelivery", "True", "False"))
    if not consumer.manual_settle_on_error:
        out.append(ProfileViolation("consumer.manual_settle_on_error", "True", "False"))
    # TODO(connection-stream): validate the connection's reconnect budget and
    # backoff here once ConnectionConfig gains the fail-fast settings; until
    # then the profile does not constrain ConnectionConfig.
    return out


def _critical_contradictions(config: RabbitConfig, defaults: RabbitConfig) -> list[str]:
    """Explicit settings the CRITICAL profile cannot reconcile."""

    def _pinned(section: Any, section_defaults: Any, name: str) -> bool:
        return bool(getattr(section, name) != getattr(section_defaults, name))

    pub = config.publisher
    out: list[str] = []
    if _pinned(pub, defaults.publisher, "max_message_bytes") and pub.max_message_bytes > CRITICAL_MAX_MESSAGE_BYTES:
        out.append(f"publisher.max_message_bytes={pub.max_message_bytes} (> 256 KiB)")
    if pub.max_message_bytes == 0:
        out.append("publisher.max_message_bytes=0 (unbounded)")
    retry = config.retry
    if retry is not None:
        if retry.delay_queue_type == "classic" and _pinned(retry, RetryConfig(), "delay_queue_type"):
            out.append("retry.delay_queue_type='classic'")  # pragma: no cover - "classic" is the default
        if retry.dlq_queue_type == "classic":
            out.append("retry.dlq_queue_type='classic'")
        if retry.delay_queue_dead_letter_strategy == DeadLetterStrategy.AT_MOST_ONCE.value:
            out.append("retry.delay_queue_dead_letter_strategy='at-most-once'")
        if retry.terminal_action is TerminalAction.REJECT:
            out.append("retry.terminal_action='reject' (no failure metadata on the DLQ copy)")
    consumer = config.consumer
    if not consumer.reject_transient_on_redelivery:
        out.append("consumer.reject_transient_on_redelivery=False")
    if not consumer.manual_settle_on_error:
        out.append("consumer.manual_settle_on_error=False")
    return out


def apply_profile(config: RabbitConfig, profile: ReliabilityProfile | str) -> RabbitConfig:
    """Return a copy of *config* satisfying *profile*'s config-level requirements.

    Raises :class:`ConfigValidationError` if the caller pinned a
    contradictory value (e.g. ``confirm_delivery=False`` under CRITICAL) —
    a profile never silently overrides an explicit choice — and, under
    CRITICAL, when ``consumer.processing_timeout`` is not set (it has no
    safe default).

    CRITICAL fills in: confirms, persistence, mandatory, body limit
    (<= 256 KiB), ``max_in_flight_bytes`` (:data:`CRITICAL_MAX_IN_FLIGHT_BYTES`
    unless set), ``strip_reserved_headers``, a ``RetryConfig`` when there is
    none (keeping your ladder when there is) with quorum delay queues
    dead-lettering at-least-once, a quorum DLQ, ``terminal_action=dead_letter``
    and ``trust_retry_header=False``. Per-route requirements (quorum source
    queues declared at-least-once, no ``ACK_FIRST``, retry not disabled) are
    the routes' to meet: check them with :func:`validate_profile` /
    :func:`preflight` after registering the subscribers. Event-metadata
    validation stays opt-in (``EventMetadataMiddleware``).

    STANDARD fills in confirms, persistence and mandatory only.
    """
    prof = ReliabilityProfile(profile)
    defaults = RabbitConfig()

    def _pinned(section: Any, section_defaults: Any, name: str) -> bool:
        return bool(getattr(section, name) != getattr(section_defaults, name))

    pub = config.publisher
    contradictions: list[str] = []
    if _pinned(pub, defaults.publisher, "confirm_delivery") and not pub.confirm_delivery:
        contradictions.append("publisher.confirm_delivery=False")
    if _pinned(pub, defaults.publisher, "persistent") and not pub.persistent:
        contradictions.append("publisher.persistent=False")
    if config.safety.reject_without_dlx == "discard":
        contradictions.append("safety.reject_without_dlx='discard'")
    if config.retry is not None and config.retry.error_detail == "raw":
        contradictions.append("retry.error_detail='raw'")
    if prof is ReliabilityProfile.CRITICAL:
        contradictions.extend(_critical_contradictions(config, defaults))
    if contradictions:
        raise ConfigValidationError(
            f"Cannot apply reliability profile {prof.value!r}: contradictory explicit settings: "
            + ", ".join(contradictions)
        )
    if prof is ReliabilityProfile.CRITICAL and config.consumer.processing_timeout is None:
        raise ConfigValidationError(f"Cannot apply reliability profile 'critical': {_PROCESSING_TIMEOUT_REQUIRED}.")

    if prof is ReliabilityProfile.CRITICAL:
        new_pub = dataclasses.replace(
            pub,
            confirm_delivery=True,
            persistent=True,
            mandatory=True,
            max_message_bytes=min(pub.max_message_bytes, CRITICAL_MAX_MESSAGE_BYTES),
            max_in_flight_bytes=(
                pub.max_in_flight_bytes if pub.max_in_flight_bytes is not None else CRITICAL_MAX_IN_FLIGHT_BYTES
            ),
            strip_reserved_headers=True,
        )
        base_retry = config.retry if config.retry is not None else RetryConfig()
        # A critical DLQ stores failed events indefinitely: always quorum
        # ("inherit" would silently become classic for a classic source).
        # Delay queues dead-letter back to the source at-least-once, so an
        # expiring retry copy is never dropped when the source is full.
        new_retry: RetryConfig | None = dataclasses.replace(
            base_retry,
            delay_queue_type="quorum",
            dlq_queue_type="quorum",
            delay_queue_dead_letter_strategy=DeadLetterStrategy.AT_LEAST_ONCE.value,
            terminal_action=TerminalAction.DEAD_LETTER,
            trust_retry_header=False,
        )
        return dataclasses.replace(config, publisher=new_pub, retry=new_retry)

    new_pub = dataclasses.replace(pub, confirm_delivery=True, persistent=True, mandatory=True)
    return dataclasses.replace(config, publisher=new_pub)


def critical_config(base: RabbitConfig | None = None) -> RabbitConfig:
    """Shorthand for ``apply_profile(base or RabbitConfig(), CRITICAL)``.

    *base* must set ``consumer.processing_timeout`` (see :func:`apply_profile`).
    """
    return apply_profile(base or RabbitConfig(), ReliabilityProfile.CRITICAL)


def standard_config(base: RabbitConfig | None = None) -> RabbitConfig:
    """Shorthand for ``apply_profile(base or RabbitConfig(), STANDARD)``."""
    return apply_profile(base or RabbitConfig(), ReliabilityProfile.STANDARD)


# ── Preflight ──────────────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class PreflightCheck:
    name: str
    status: PreflightStatus
    detail: str = ""


@dataclass(frozen=True, slots=True)
class PreflightReport:
    profile: ReliabilityProfile
    checks: tuple[PreflightCheck, ...] = field(default_factory=tuple)

    def by_status(self, status: PreflightStatus) -> tuple[PreflightCheck, ...]:
        return tuple(c for c in self.checks if c.status is status)

    @property
    def failed(self) -> tuple[PreflightCheck, ...]:
        return self.by_status(PreflightStatus.FAILED)

    @property
    def unverified(self) -> tuple[PreflightCheck, ...]:
        return self.by_status(PreflightStatus.UNVERIFIED)

    @property
    def ok(self) -> bool:
        """No FAILED checks. UNVERIFIED checks do NOT make this False — they
        are listed separately and must be reported, not hidden."""
        return not self.failed

    @property
    def fully_verified(self) -> bool:
        return self.ok and not self.unverified

    def as_dict(self) -> dict[str, Any]:
        return {
            "profile": self.profile.value,
            "ok": self.ok,
            "fully_verified": self.fully_verified,
            "checks": [{"name": c.name, "status": c.status.value, "detail": c.detail} for c in self.checks],
        }


def _queue_policy_definition(info: dict[str, Any]) -> dict[str, Any]:
    """Effective policy definition + declared arguments, merged (policy wins)."""
    merged: dict[str, Any] = {}
    args = info.get("arguments") or {}
    if isinstance(args, dict):
        merged.update(args)
    eff = info.get("effective_policy_definition") or {}
    if isinstance(eff, dict):
        merged.update(eff)
    return merged


def preflight(
    config: RabbitConfig,
    profile: ReliabilityProfile | str,
    *,
    routes: Sequence[Any] = (),
    management_client: Any | None = None,
    vhost: str = "/",
) -> PreflightReport:
    """Verify profile prerequisites locally and, when a read-only management
    client is provided, on the broker. Broker-side checks are reported
    ``UNVERIFIED`` (with the reason) when no client is given or a lookup
    fails — never silently VERIFIED.

    *management_client* needs only ``get_queue(name, vhost)`` returning the
    management API's queue JSON (``RabbitManagementClient`` fits).
    """
    prof = ReliabilityProfile(profile)
    checks: list[PreflightCheck] = []

    violations = validate_profile(config, prof, routes=routes)
    if management_client is not None:
        # The broker-side dead-letter-strategy check below is authoritative
        # (it sees policies too); drop the local "not declared" warning.
        violations = [
            v
            for v in violations
            if not (v.setting.startswith("queue[") and v.setting.endswith(".dead_letter_strategy"))
        ]
    if violations:
        for v in violations:
            checks.append(
                PreflightCheck(
                    name=f"config:{v.setting}",
                    status=PreflightStatus.FAILED if v.severity == "error" else PreflightStatus.UNVERIFIED,
                    detail=str(v),
                )
            )
    else:
        checks.append(PreflightCheck(name="config", status=PreflightStatus.VERIFIED, detail="profile requirements met"))

    queues = _route_queues(routes)
    if prof is ReliabilityProfile.CRITICAL and queues:
        for q in queues:
            name = str(getattr(q, "name", ""))
            if management_client is None:
                checks.append(
                    PreflightCheck(
                        name=f"broker:{name}",
                        status=PreflightStatus.UNVERIFIED,
                        detail="no management client supplied; broker policy not checked",
                    )
                )
                continue
            try:
                info = management_client.get_queue(name, vhost)
            except Exception as exc:  # 404 (not declared yet), auth, network
                checks.append(
                    PreflightCheck(
                        name=f"broker:{name}",
                        status=PreflightStatus.UNVERIFIED,
                        detail=f"management lookup failed: {type(exc).__name__}",
                    )
                )
                continue
            if not isinstance(info, dict):
                checks.append(
                    PreflightCheck(
                        name=f"broker:{name}", status=PreflightStatus.UNVERIFIED, detail="unexpected management payload"
                    )
                )
                continue
            qtype = str(info.get("type") or _queue_policy_definition(info).get("x-queue-type") or "classic")
            checks.append(
                PreflightCheck(
                    name=f"broker:{name}:type",
                    status=PreflightStatus.VERIFIED if qtype == "quorum" else PreflightStatus.FAILED,
                    detail=f"type={qtype}",
                )
            )
            pol = _queue_policy_definition(info)
            dls = pol.get("dead-letter-strategy") or pol.get("x-dead-letter-strategy")
            checks.append(
                PreflightCheck(
                    name=f"broker:{name}:dead-letter-strategy",
                    status=PreflightStatus.VERIFIED if dls == "at-least-once" else PreflightStatus.FAILED,
                    detail=f"dead-letter-strategy={dls!r}",
                )
            )
            overflow = pol.get("overflow") or pol.get("x-overflow")
            checks.append(
                PreflightCheck(
                    name=f"broker:{name}:overflow",
                    status=PreflightStatus.VERIFIED if overflow == "reject-publish" else PreflightStatus.FAILED,
                    detail=f"overflow={overflow!r}",
                )
            )
            limit = pol.get("delivery-limit") or pol.get("x-delivery-limit")
            checks.append(
                PreflightCheck(
                    name=f"broker:{name}:delivery-limit",
                    status=PreflightStatus.VERIFIED if isinstance(limit, int) and limit > 0 else PreflightStatus.FAILED,
                    detail=f"delivery-limit={limit!r}",
                )
            )
    return PreflightReport(profile=prof, checks=tuple(checks))


# ── Policy templates ───────────────────────────────────────────────────────


@dataclass(frozen=True, slots=True)
class PolicyTemplate:
    """One RabbitMQ policy in management-API shape (``PUT /api/policies/{vhost}/{name}``)."""

    name: str
    pattern: str
    definition: dict[str, Any]
    apply_to: str = "quorum_queues"
    priority: int = 10
    vhost: str = "/"

    def as_api_body(self) -> dict[str, Any]:
        return {
            "pattern": self.pattern,
            "definition": dict(self.definition),
            "apply-to": self.apply_to,
            "priority": self.priority,
        }

    def as_rabbitmqctl(self) -> str:
        import json

        return (
            f"rabbitmqctl set_policy -p {self.vhost} --priority {self.priority} --apply-to {self.apply_to} "
            f"{self.name} '{self.pattern}' '{json.dumps(self.definition, separators=(',', ':'))}'"
        )


def _regex_escape(name: str) -> str:
    import re

    return re.escape(name)


def policy_templates(
    queue_names: Sequence[str],
    *,
    retry: RetryConfig | None = None,
    profile: ReliabilityProfile | str = ReliabilityProfile.CRITICAL,
    delivery_limit: int | None = None,
    max_length: int | None = None,
    vhost: str = "/",
) -> list[PolicyTemplate]:
    """Render reviewed policy definitions for *queue_names* and their retry chain.

    For the CRITICAL profile every source queue and DLQ gets
    ``dead-letter-strategy: at-least-once`` (which RabbitMQ requires to be
    paired with ``overflow: reject-publish``) and a ``delivery-limit``
    backstop (defaults to ``retry.max_retries + 1`` when *retry* is given,
    else 5). Delay queues get only ``overflow: reject-publish`` — TTL expiry
    is their whole job and they must never drop on overflow. STANDARD
    renders just the overflow guard. Nothing here is applied automatically.
    """
    prof = ReliabilityProfile(profile)
    limit = delivery_limit if delivery_limit is not None else ((retry.max_retries + 1) if retry is not None else 5)
    out: list[PolicyTemplate] = []
    for q in queue_names:
        src_def: dict[str, Any] = {"overflow": "reject-publish"}
        if prof is ReliabilityProfile.CRITICAL:
            src_def["dead-letter-strategy"] = "at-least-once"
            src_def["delivery-limit"] = limit
        if max_length is not None:
            src_def["max-length"] = max_length
        out.append(
            PolicyTemplate(
                name=f"rabbitkit-{q}-source",
                pattern=f"^{_regex_escape(q)}$",
                definition=src_def,
                apply_to="quorum_queues" if prof is ReliabilityProfile.CRITICAL else "queues",
                vhost=vhost,
            )
        )
        if retry is not None:
            out.append(
                PolicyTemplate(
                    name=f"rabbitkit-{q}-retry",
                    pattern=f"^{_regex_escape(q)}\\.retry\\.[0-9]+(\\.s[0-9]+)?$",
                    definition={"overflow": "reject-publish"},
                    apply_to="queues",
                    vhost=vhost,
                )
            )
            dlq_def: dict[str, Any] = {"overflow": "reject-publish"}
            if max_length is not None:
                dlq_def["max-length"] = max_length
            out.append(
                PolicyTemplate(
                    name=f"rabbitkit-{q}-dlq",
                    pattern=f"^{_regex_escape(q)}\\.dlq$",
                    definition=dlq_def,
                    apply_to="quorum_queues" if prof is ReliabilityProfile.CRITICAL else "queues",
                    vhost=vhost,
                )
            )
    return out
