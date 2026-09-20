"""Reliability profiles, preflight verification, and broker policy templates.

Two explicit profiles (plan §9):

=====================  ==============================  ==========================================
Setting                STANDARD                        CRITICAL
=====================  ==============================  ==========================================
Confirms               on                              required
Mandatory routing      on                              required
Persistence            on                              required
Queue type             explicit choice                 quorum for every durable route queue
Retry topology         explicit                        quorum delay chain + quorum DLQ
Body limit             explicit                        <= 256 KiB
Error text             sanitized                       sanitized or omitted (never raw)
Dead-letter path       auto-provisioned                never ``discard``
=====================  ==============================  ==========================================

Profiles are opt-in. :func:`apply_profile` returns a NEW ``RabbitConfig``
with the profile's requirements filled in and raises on a *contradiction*
(a setting the caller pinned explicitly to a value the profile forbids) —
it never silently flips a value you set on purpose. :func:`validate_profile`
just reports.

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
    PublisherConfig,
    RabbitConfig,
    RetryConfig,
    SafetyConfig,
)
from rabbitkit.core.errors import ConfigValidationError
from rabbitkit.core.types import PreflightStatus, QueueType, ReliabilityProfile

#: Team baseline body limit for the critical profile (256 KiB).
CRITICAL_MAX_MESSAGE_BYTES = 256 * 1024


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
    if retry is not None and retry.error_detail == "raw":
        out.append(ProfileViolation("retry.error_detail", "'sanitized' or 'omit'", "'raw'"))

    if prof is ReliabilityProfile.CRITICAL:
        if pub.max_message_bytes > CRITICAL_MAX_MESSAGE_BYTES:
            out.append(
                ProfileViolation(
                    "publisher.max_message_bytes",
                    f"<= {CRITICAL_MAX_MESSAGE_BYTES}",
                    str(pub.max_message_bytes),
                )
            )
        if retry is None:
            out.append(ProfileViolation("retry", "RetryConfig(...) (bounded retry + DLQ)", "None"))
        else:
            if retry.delay_queue_type != "quorum":
                out.append(ProfileViolation("retry.delay_queue_type", "'quorum'", repr(retry.delay_queue_type)))
            if retry.dlq_queue_type not in ("quorum", "inherit"):
                out.append(
                    ProfileViolation("retry.dlq_queue_type", "'quorum' or 'inherit'", repr(retry.dlq_queue_type))
                )
        for q in _route_queues(routes):
            qtype = getattr(q, "queue_type", None)
            durable = getattr(q, "durable", True)
            if durable and qtype is not QueueType.QUORUM:
                out.append(
                    ProfileViolation(
                        f"queue[{getattr(q, 'name', '?')}].queue_type",
                        "QueueType.QUORUM",
                        str(getattr(qtype, "value", qtype)),
                    )
                )
            if retry is not None and retry.dlq_queue_type == "inherit" and qtype is not QueueType.QUORUM:
                out.append(
                    ProfileViolation(
                        f"queue[{getattr(q, 'name', '?')}].dlq",
                        "quorum DLQ (source quorum or dlq_queue_type='quorum')",
                        "classic (inherited)",
                    )
                )
    return out


def apply_profile(config: RabbitConfig, profile: ReliabilityProfile | str) -> RabbitConfig:
    """Return a copy of *config* satisfying *profile*'s publisher/safety/retry
    requirements. Raises :class:`ConfigValidationError` if the caller pinned
    a contradictory value (e.g. ``confirm_delivery=False`` under CRITICAL) —
    a profile never silently overrides an explicit choice.
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
        if _pinned(pub, defaults.publisher, "max_message_bytes") and pub.max_message_bytes > CRITICAL_MAX_MESSAGE_BYTES:
            contradictions.append(f"publisher.max_message_bytes={pub.max_message_bytes} (> 256 KiB)")
        if pub.max_message_bytes == 0:
            contradictions.append("publisher.max_message_bytes=0 (unbounded)")
        if (
            config.retry is not None
            and config.retry.delay_queue_type == "classic"
            and _pinned(config.retry, RetryConfig(), "delay_queue_type")
        ):
            contradictions.append("retry.delay_queue_type='classic'")
        if config.retry is not None and config.retry.dlq_queue_type == "classic":
            contradictions.append("retry.dlq_queue_type='classic'")
    if contradictions:
        raise ConfigValidationError(
            f"Cannot apply reliability profile {prof.value!r}: contradictory explicit settings: "
            + ", ".join(contradictions)
        )

    new_pub = dataclasses.replace(
        pub,
        confirm_delivery=True,
        persistent=True,
        mandatory=True,
        max_message_bytes=(
            min(pub.max_message_bytes, CRITICAL_MAX_MESSAGE_BYTES)
            if prof is ReliabilityProfile.CRITICAL
            else pub.max_message_bytes
        ),
    )
    new_retry = config.retry
    if prof is ReliabilityProfile.CRITICAL:
        base_retry = config.retry if config.retry is not None else RetryConfig()
        # A critical DLQ stores failed events indefinitely: always quorum
        # ("inherit" would silently become classic for a classic source).
        new_retry = dataclasses.replace(base_retry, delay_queue_type="quorum", dlq_queue_type="quorum")
    return dataclasses.replace(config, publisher=new_pub, retry=new_retry)


def critical_config(base: RabbitConfig | None = None) -> RabbitConfig:
    """Shorthand for ``apply_profile(base or RabbitConfig(), CRITICAL)``."""
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
