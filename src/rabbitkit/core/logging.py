"""Structured logging configuration for rabbitkit.

structlog is a declared dependency but must be explicitly configured.
``LoggingConfig`` controls rendering (JSON for prod, console for dev).
Once configured, all rabbitkit internals (pipeline, broker, transport) emit
structured log events with per-message context automatically bound via
``structlog.contextvars``.

Usage
-----
Pass ``logging=LoggingConfig(...)`` to ``RabbitConfig`` and the broker will
call ``configure_structlog()`` on ``start()``:

    from rabbitkit import RabbitConfig
    from rabbitkit.core.logging import LoggingConfig
    from rabbitkit.async_ import AsyncBroker

    # Development — coloured console output
    broker = AsyncBroker(
        RabbitConfig(
            logging=LoggingConfig(render_json=False, include_caller_info=True)
        )
    )

    # Production — JSON lines to stdout (pipe to fluentd / Loki / etc.)
    broker = AsyncBroker(
        RabbitConfig(
            logging=LoggingConfig(render_json=True, timestamper_fmt="iso")
        )
    )

Per-message context
-------------------
The pipeline automatically binds these keys for every message:

    message_id, routing_key, queue, handler

They appear in every log line emitted while the handler runs and are cleared
in a ``finally`` block so they never bleed into unrelated events.

Manual configuration
--------------------
Call ``configure_structlog()`` directly if you manage the broker lifecycle
yourself and do not use ``RabbitConfig.logging``:

    from rabbitkit.core.logging import configure_structlog, LoggingConfig

    configure_structlog(LoggingConfig(render_json=True))

Safe to call multiple times — last call wins.

Secrets and message content (L16)
----------------------------------
rabbitkit's own structured log events never include the message body or
the raw ``headers`` dict — only ``message_id``, ``routing_key``, ``queue``,
and ``handler`` are bound per message. Bodies/headers may legitimately
carry credentials or PII, so this is deliberate: none of rabbitkit's
internal logging can leak them.

That guarantee does not extend to log calls YOU write. If your own
handler code does e.g. ``logger.info("processing", headers=msg.headers)``,
whatever is in that dict goes out verbatim. Because ``configure_structlog()``
sets structlog's *global* processor chain, ``LoggingConfig.redact_keys``
(enabled by default) applies to those calls too: any top-level event field,
or field one level deep inside a nested dict (e.g. ``headers={...}``),
whose key case-insensitively matches an entry in ``redact_keys`` is
replaced with a fixed redacted marker before rendering. This is a
best-effort, key-name-based scrubber — not a PII/content scanner, and not a
substitute for simply not logging bodies/headers containing secrets in the
first place. Pass ``redact_keys=None`` to disable it, or a custom
``frozenset`` to redact your own key names instead of (or in addition to)
the defaults.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import structlog

# L16: common credential/secret-bearing key names, matched case-insensitively.
# Deliberately name-based (not content-based) -- see the module docstring.
DEFAULT_REDACT_KEYS: frozenset[str] = frozenset(
    {
        "password",
        "passwd",
        "secret",
        "token",
        "api_key",
        "apikey",
        "authorization",
        "auth",
        "access_token",
        "refresh_token",
        "private_key",
        "client_secret",
    }
)

_REDACTED = "***REDACTED***"


@dataclass(frozen=True, slots=True)
class LoggingConfig:
    """Structured logging configuration.

    Attributes:
        render_json: True for JSON output (prod), False for console (dev).
        add_log_level: Include log level in output.
        timestamper_fmt: Timestamp format ("iso", "unix", or None to disable).
        include_caller_info: Add filename/line number to log events.
        redact_keys: Key names to redact from log events -- checked at the
            top level and one level deep inside nested dict values (e.g. a
            ``headers={...}`` field). Matching is case-insensitive and
            normalizes AMQP-style ``x-`` prefixes/hyphens, so ``api_key``
            also matches ``X-Api-Key``. Defaults to
            :data:`DEFAULT_REDACT_KEYS`. Pass ``None`` to disable redaction
            entirely, or your own ``frozenset`` to customize it. See the
            module docstring ("Secrets and message content") for scope and
            limitations.
    """

    render_json: bool = False
    add_log_level: bool = True
    timestamper_fmt: str = "iso"
    include_caller_info: bool = False
    redact_keys: frozenset[str] | None = DEFAULT_REDACT_KEYS


def _normalize_key(key: str) -> str:
    """Normalize a key for comparison (L16).

    AMQP headers conventionally use a ``x-`` prefix and hyphens (e.g.
    ``x-api-key``), not the Python-style snake_case of
    :data:`DEFAULT_REDACT_KEYS` (``api_key``). Stripping the ``x-`` prefix
    and folding hyphens to underscores lets both spellings match the same
    default entry.
    """
    lowered = key.lower()
    if lowered.startswith("x-"):
        lowered = lowered[2:]
    return lowered.replace("-", "_")


def _redact_processor(keys: frozenset[str]) -> Any:
    """Build a structlog processor that redacts *keys* (L16).

    Checks event-dict keys at the top level and one level deep inside any
    nested ``dict`` value (covers the common ``headers={...}`` shape),
    normalized via :func:`_normalize_key`. Not a recursive/deep scan -- see
    the module docstring for why a shallow, name-based approach is the
    deliberate scope here.
    """
    normalized_keys = {_normalize_key(k) for k in keys}

    def processor(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        for key, value in event_dict.items():
            if _normalize_key(key) in normalized_keys:
                event_dict[key] = _REDACTED
            elif isinstance(value, dict):
                event_dict[key] = {
                    nested_key: (_REDACTED if _normalize_key(nested_key) in normalized_keys else nested_value)
                    for nested_key, nested_value in value.items()
                }
        return event_dict

    return processor


def configure_structlog(config: LoggingConfig | None = None) -> None:
    """One-time structlog configuration.

    Safe to call multiple times — last call wins.
    If config is None, uses defaults (console renderer, ISO timestamps).
    """
    import structlog

    if config is None:
        config = LoggingConfig()

    processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
    ]

    if config.redact_keys:
        processors.append(_redact_processor(config.redact_keys))

    if config.add_log_level:
        processors.append(structlog.stdlib.add_log_level)

    if config.timestamper_fmt:
        fmt = config.timestamper_fmt if config.timestamper_fmt != "iso" else "iso"
        processors.append(structlog.processors.TimeStamper(fmt=fmt))

    if config.include_caller_info:
        processors.append(structlog.processors.CallsiteParameterAdder())

    processors.append(structlog.stdlib.PositionalArgumentsFormatter())
    processors.append(structlog.processors.StackInfoRenderer())
    processors.append(structlog.processors.UnicodeDecoder())

    if config.render_json:
        processors.append(structlog.processors.JSONRenderer())
    else:
        processors.append(structlog.dev.ConsoleRenderer())

    structlog.configure(
        processors=processors,
        wrapper_class=structlog.stdlib.BoundLogger,
        context_class=dict,
        logger_factory=structlog.stdlib.LoggerFactory(),
        cache_logger_on_first_use=True,
    )
