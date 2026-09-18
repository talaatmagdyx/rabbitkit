"""ErrorSanitizer — bounded, secret-free error metadata for retry/DLQ headers.

Raw ``str(exc)`` routinely embeds things that must never travel on a
message header or a log line: AMQP URLs with passwords, bearer tokens,
``password=...`` fragments from an ORM error, API keys echoed by an HTTP
client. Headers on a dead-lettered message outlive log retention and are
visible to anyone with management-UI read access — so the terminal
metadata rabbitkit writes is an allowlisted category, a bounded code, and
(optionally) a redacted, length-capped summary. Never the raw text.

Three policies (``RetryConfig.error_detail``):

* ``"sanitized"`` (default) — category + code + redacted summary.
* ``"omit"`` — category + code only; no message text at all.
* ``"raw"`` — legacy behavior (length-capped ``str(exc)``). Opt-in only.
"""

from __future__ import annotations

import re
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

from rabbitkit.core.types import ErrorSeverity

#: Error text policies accepted by ``RetryConfig.error_detail``.
ERROR_DETAIL_POLICIES: tuple[str, ...] = ("sanitized", "omit", "raw")

_REDACTED = "***"

# Order matters: URL credentials first (they contain ':' and '@' that later
# generic patterns would otherwise partially eat).
_DEFAULT_PATTERNS: tuple[re.Pattern[str], ...] = (
    # scheme://user:password@host  → scheme://***:***@host
    # password is greedy up to the LAST "@" before the host so an unescaped
    # "@" inside the secret cannot leak its tail
    re.compile(r"(?i)\b([a-z][a-z0-9+.-]*://)([^/\s:@]+):([^/\s]+)@"),
    # Authorization: Bearer <token> / Basic <blob>
    re.compile(r"(?i)\b(bearer|basic)\s+[A-Za-z0-9\-._~+/]+=*"),
    # key=value / key: value / "key": "value" for secret-bearing key names
    re.compile(
        r"(?i)\b((?:x-)?(?:password|passwd|pwd|secret(?:[_-]?key)?|token|api[_-]?key|access[_-]?key|"
        r"private[_-]?key|client[_-]?secret|auth(?:orization)?|credential[s]?|session[_-]?id))"
        r"([\"']?\s*[:=]\s*[\"']?)([^\s,;\"'&]+)"
    ),
    # AWS access key ids
    re.compile(r"\bAKIA[0-9A-Z]{16}\b"),
    # Long opaque hex / base64-ish blobs (>= 32 chars) — typical tokens/hashes
    re.compile(r"\b[A-Fa-f0-9]{32,}\b"),
    re.compile(r"\b[A-Za-z0-9+/_-]{40,}={0,2}\b"),
)

_CODE_SAFE = re.compile(r"[^A-Za-z0-9_.]")


@dataclass(frozen=True, slots=True)
class SanitizedError:
    """Allowlisted, bounded description of a failure.

    Attributes:
        category: ``"transient"`` / ``"permanent"`` / ``"unknown"``.
        code: Exception class name, restricted to ``[A-Za-z0-9_.]`` and
            capped in length — stable enough to alert on, never user text.
        summary: Redacted, length-capped message text (empty under ``omit``).
    """

    category: str
    code: str
    summary: str = ""

    def as_headers(self, prefix: str = "x-rabbitkit-error") -> dict[str, str]:
        headers = {f"{prefix}-category": self.category, f"{prefix}-type": self.code}
        if self.summary:
            headers[f"{prefix}-message"] = self.summary
        return headers


class ErrorSanitizer:
    """Turn an exception into a :class:`SanitizedError`.

    Args:
        policy: One of :data:`ERROR_DETAIL_POLICIES`.
        max_summary_len: Hard cap on the summary (after redaction).
        max_code_len: Hard cap on the code.
        patterns: Regexes whose matches are replaced by ``***`` in the
            summary. Defaults cover URL credentials, bearer/basic auth,
            ``password=``/``token=``-style pairs, AWS key ids, and long
            opaque tokens.
        allowed_codes: Optional allowlist of exception class names. A code
            outside it is reported as ``"ApplicationError"`` — for teams
            that consider exception class names themselves sensitive.
    """

    def __init__(
        self,
        *,
        policy: str = "sanitized",
        max_summary_len: int = 256,
        max_code_len: int = 64,
        patterns: Sequence[re.Pattern[str]] | None = None,
        allowed_codes: frozenset[str] | None = None,
    ) -> None:
        if policy not in ERROR_DETAIL_POLICIES:
            raise ValueError(f"policy must be one of {ERROR_DETAIL_POLICIES}, got {policy!r}")
        if max_summary_len < 0 or max_code_len < 1:
            raise ValueError("max_summary_len must be >= 0 and max_code_len >= 1")
        self._policy = policy
        self._max_summary = max_summary_len
        self._max_code = max_code_len
        self._patterns = tuple(patterns) if patterns is not None else _DEFAULT_PATTERNS
        self._allowed_codes = allowed_codes

    @property
    def policy(self) -> str:
        return self._policy

    # ── public API ────────────────────────────────────────────────────────

    def code_for(self, exc: BaseException) -> str:
        name = type(exc).__name__
        if self._allowed_codes is not None and name not in self._allowed_codes:
            name = "ApplicationError"
        return _CODE_SAFE.sub("", name)[: self._max_code] or "Error"

    def redact(self, text: str) -> str:
        """Apply every redaction pattern to *text*."""
        out = text
        for pat in self._patterns:
            out = pat.sub(self._replacement_for(pat), out)
        return out

    def summary_for(self, exc: BaseException) -> str:
        if self._policy == "omit" or self._max_summary == 0:
            return ""
        try:
            text = str(exc)
        except Exception:
            text = "<unprintable exception>"
        if self._policy == "raw":
            return text[: self._max_summary]
        # Collapse whitespace/newlines: a header is one line; a stack-trace-
        # shaped message must not smuggle formatting into log pipelines.
        text = " ".join(text.split())
        return self.redact(text)[: self._max_summary]

    def sanitize(self, exc: BaseException, severity: ErrorSeverity | None = None) -> SanitizedError:
        category = severity.value if severity is not None else "unknown"
        return SanitizedError(category=category, code=self.code_for(exc), summary=self.summary_for(exc))

    # ── internals ────────────────────────────────────────────────────────

    @staticmethod
    def _replacement_for(pat: re.Pattern[str]) -> Any:
        groups = pat.groups
        if groups == 3 and "://" in pat.pattern:
            return rf"\1{_REDACTED}:{_REDACTED}@"
        if groups == 3:
            return rf"\1\2{_REDACTED}"
        if groups == 1:
            return rf"\1 {_REDACTED}"
        return _REDACTED


def contains_secret_marker(text: str, secrets: Sequence[str]) -> bool:
    """Test helper: True if any literal secret fixture appears in *text*."""
    return any(s and s in text for s in secrets)
