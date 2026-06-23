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
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class LoggingConfig:
    """Structured logging configuration.

    Attributes:
        render_json: True for JSON output (prod), False for console (dev).
        add_log_level: Include log level in output.
        timestamper_fmt: Timestamp format ("iso", "unix", or None to disable).
        include_caller_info: Add filename/line number to log events.
    """

    render_json: bool = False
    add_log_level: bool = True
    timestamper_fmt: str = "iso"
    include_caller_info: bool = False


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
