# NO from __future__ import annotations — intentional.
# Handlers here have real (non-string) type annotations so that
# inspect.signature() returns actual type objects, allowing
# HandlerPipeline._get_body_type() branches that compare
# ann is RabbitMessage, check __metadata__, etc. to be exercised.

from typing import Annotated

from rabbitkit.core.message import RabbitMessage


def handler_no_annotation(body) -> None:  # type: ignore[no-untyped-def]
    """No annotation on body — triggers Parameter.empty branch (line 263)."""
    pass


def handler_rabbit_message(msg: RabbitMessage) -> None:
    """RabbitMessage annotation — triggers line 267 continue."""
    pass


def handler_annotated_param(dep: Annotated[str, "di-marker"]) -> None:
    """Annotated param — triggers __metadata__ branch (line 271)."""
    pass


def handler_rabbit_message_body(msg: RabbitMessage, body: bytes) -> None:
    """RabbitMessage first param, then body — msg is injected via line 301."""
    pass


def handler_bytes(body: bytes) -> None:
    """bytes-annotated handler — _get_body_type returns bytes class (not str)."""
    pass
