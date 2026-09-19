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
whose key case-insensitively matches -- or contains all the underscore-
separated words of -- an entry in ``redact_keys`` is replaced with a fixed
redacted marker before rendering. The word-based matching is what catches
compound key names like ``x-auth-token`` or ``session-token`` against the
standalone ``token``/``auth`` defaults, without treating a partial word
from a compound default (e.g. ``key`` from ``api_key``) as a standalone
matcher -- that would misfire on unrelated fields like ``primary_key``.
This is a best-effort, key-name-based scrubber — not a PII/content
scanner, and not a substitute for simply not logging bodies/headers
containing secrets in the first place. Pass ``redact_keys=None`` to
disable it, or a custom ``frozenset`` to redact your own key names instead
of (or in addition to) the defaults.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from rabbitkit.core.sanitizer import ErrorSanitizer

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
        capture_warnings: Route Python's ``warnings`` module (used for every
            rabbitkit safety warning -- topology drift, retry-without-
            confirms, unsafe TLS, dashboard auth, ...) through the standard
            ``logging`` module via ``logging.captureWarnings()``. Without
            this, ``warnings.warn()`` writes directly to ``sys.stderr`` in
            its own format, completely bypassing whatever log pipeline
            ``render_json``/handlers were set up for -- a "loud warning" is
            only actually loud if something is watching raw stderr in dev;
            in a production JSON-logging deployment it's invisible unless
            this is enabled. Default ``True``. Set ``False`` if your
            application already manages ``captureWarnings`` itself.
    """

    render_json: bool = False
    add_log_level: bool = True
    timestamper_fmt: str = "iso"
    include_caller_info: bool = False
    redact_keys: frozenset[str] | None = DEFAULT_REDACT_KEYS
    capture_warnings: bool = True


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

    Matching a normalized key against *keys* is a word-set match, not an
    exact-string match: a configured key may itself be a compound
    (``api_key``, ``access_token``), so a target key matches when it
    contains ALL of some configured key's underscore-separated words (in
    any order) -- e.g. ``x-auth-token`` normalizes to ``auth_token``,
    which contains the words of the standalone ``token`` entry, so it
    matches even though ``auth_token`` itself isn't literally one of the
    configured names. Exact-string matching alone let common compound
    secret-bearing names (``x-auth-token``, ``session-token``,
    ``x-secret-key``, ``bearer-token``, ...) slip through untouched.
    Splitting each configured key into its OWN word-set (rather than
    pooling every word from every configured key into one flat set) is
    what keeps this from over-matching: naively treating ``api_key``'s
    ``key`` as a standalone matcher would redact totally benign fields
    like ``primary_key``/``cache_key``, since neither contains ``api``.
    """
    redact_word_sets = [frozenset(_normalize_key(k).split("_")) for k in keys]

    def matches(normalized_key: str) -> bool:
        key_words = frozenset(normalized_key.split("_"))
        return any(words <= key_words for words in redact_word_sets)

    def processor(logger: Any, method_name: str, event_dict: dict[str, Any]) -> dict[str, Any]:
        for key, value in event_dict.items():
            if matches(_normalize_key(key)):
                event_dict[key] = _REDACTED
            elif isinstance(value, dict):
                event_dict[key] = {
                    nested_key: (_REDACTED if matches(_normalize_key(nested_key)) else nested_value)
                    for nested_key, nested_value in value.items()
                }
        return event_dict

    return processor



# ── Value-level redaction for STDLIB logging (0.16) ───────────────────────


class SecretRedactingFilter(logging.Filter):
    """Redact credential-shaped VALUES from rendered log records.

    ``_redact_processor`` above scrubs by key NAME and only under structlog.
    Two gaps followed from that, both of which leaked plaintext:

    1. Only four rabbitkit modules use structlog; **thirty-four use
       :mod:`logging` directly** and bypassed the processor entirely.
    2. Key-name matching cannot catch a secret in the VALUE of an innocently
       named field. ``logger.warning("connection lost", error=str(exc))`` has
       the key ``error``, so an exception carrying
       ``amqp://svc:pw@host`` sailed through.

    ``ErrorSanitizer`` already knew how to redact these, but it had exactly
    one call site in the whole package — the dead-letter header — so
    ``error_detail="sanitized"`` protected the header while the log line,
    right next to it, printed the password.

    This filter closes both by redacting the *formatted* message. Attach it
    to the ``rabbitkit`` logger and it covers every module regardless of
    which logging API they use.

    It does NOT redact tracebacks: those are rendered by the Formatter, not
    the record. Use :class:`SecretRedactingFormatter` for that.
    """

    def __init__(self, sanitizer: ErrorSanitizer | None = None, name: str = "") -> None:
        super().__init__(name)
        self._sanitizer = sanitizer or ErrorSanitizer()

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            rendered = record.getMessage()
        except Exception:  # pragma: no cover - a broken record must still log
            return True
        redacted = self._sanitizer.redact(rendered)
        if redacted != rendered:
            # Replace msg wholesale and drop args: the substitution already
            # happened, and re-applying args would raise or re-introduce the
            # secret.
            record.msg = redacted
            record.args = ()
        return True


class SecretRedactingFormatter(logging.Formatter):
    """A Formatter that also redacts the rendered TRACEBACK.

    The last line of a traceback is ``str(exc)``, so any handler raising
    ``SQLAlchemyError("could not connect: postgres://svc:pw@db/app")`` put
    the password into every ``logger.exception(...)`` and every
    ``exc_info=True`` call — of which rabbitkit has roughly forty.
    """

    def __init__(self, *args: Any, sanitizer: ErrorSanitizer | None = None, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._sanitizer = sanitizer or ErrorSanitizer()

    def formatException(self, ei: Any) -> str:  # noqa: N802 — stdlib signature
        return self._sanitizer.redact(super().formatException(ei))

    def format(self, record: logging.LogRecord) -> str:
        return self._sanitizer.redact(super().format(record))


def install_log_redaction(
    logger_name: str = "rabbitkit",
    *,
    sanitizer: ErrorSanitizer | None = None,
) -> SecretRedactingFilter:
    """Attach :class:`SecretRedactingFilter` to *logger_name*, idempotently.

    Called automatically by :func:`configure_structlog`. Call it directly if
    you do not use rabbitkit's logging setup but still want its log lines
    scrubbed.
    """
    root = logging.getLogger(logger_name)
    for existing in root.filters:
        if isinstance(existing, SecretRedactingFilter):
            return existing
    filt = SecretRedactingFilter(sanitizer)

    # A Filter on a Logger runs only for records logged DIRECTLY on it.
    # Records from child loggers propagate to ancestor HANDLERS and skip
    # ancestor filters entirely, so attaching to "rabbitkit" alone would
    # miss every `logging.getLogger(__name__)` in the package — which is 34
    # of the 38 modules. Attach to each logger in the namespace instead.
    root.addFilter(filt)
    prefix = f"{logger_name}."
    for name, logger in list(logging.Logger.manager.loggerDict.items()):
        if not name.startswith(prefix) or not isinstance(logger, logging.Logger):
            continue
        if not any(isinstance(f, SecretRedactingFilter) for f in logger.filters):
            logger.addFilter(filt)

    # Any handler already attached under this namespace gets it too, which
    # also covers loggers created AFTER this call whose records reach one of
    # those handlers.
    for logger in (root, *(
        lg
        for name, lg in logging.Logger.manager.loggerDict.items()
        if name.startswith(prefix) and isinstance(lg, logging.Logger)
    )):
        for handler in logger.handlers:
            if not any(isinstance(f, SecretRedactingFilter) for f in handler.filters):
                handler.addFilter(filt)
    return filt


def configure_structlog(config: LoggingConfig | None = None) -> None:
    """One-time structlog configuration.

    Safe to call multiple times — last call wins.
    If config is None, uses defaults (console renderer, ISO timestamps).
    """
    import logging

    import structlog

    if config is None:
        config = LoggingConfig()

    # L16 follow-up: bridge warnings.warn() (every rabbitkit safety warning)
    # into the standard logging module -- otherwise it bypasses this whole
    # pipeline entirely, writing straight to sys.stderr in its own format.
    logging.captureWarnings(config.capture_warnings)

    processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_logger_name,
    ]

    if config.redact_keys:
        processors.append(_redact_processor(config.redact_keys))
        # Key-name redaction only reaches structlog callers (4 of 38 modules)
        # and cannot see a secret in the VALUE of an innocently named field.
        # This covers every rabbitkit logger regardless of logging API.
        install_log_redaction()

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
