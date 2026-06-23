"""Tests for Pydantic auto-validation in core/pipeline.py _deserialize_body.

NOTE: This file intentionally does NOT use `from __future__ import annotations`
so that handler type annotations resolve to actual classes (needed for
_get_body_type to return real types instead of strings).
"""

from unittest.mock import MagicMock

from rabbitkit.core.message import RabbitMessage
from rabbitkit.core.pipeline import HandlerPipeline
from rabbitkit.core.route import RouteDefinition
from rabbitkit.core.topology import RabbitExchange, RabbitQueue

# ── helpers ───────────────────────────────────────────────────────────────


def _make_message(**kwargs: object) -> RabbitMessage:
    defaults: dict[str, object] = {"body": b'{"name": "Alice", "age": 30}', "routing_key": "test"}
    defaults.update(kwargs)
    return RabbitMessage(**defaults)  # type: ignore[arg-type]


def _wire_sync(msg: RabbitMessage) -> None:
    msg._ack_fn = MagicMock()
    msg._nack_fn = MagicMock()
    msg._reject_fn = MagicMock()


def _make_route(handler, **kwargs):  # type: ignore[no-untyped-def]
    defaults = {
        "name": "test-route",
        "queue": RabbitQueue(name="test-queue"),
        "exchange": RabbitExchange(name="test-exchange"),
        "handler": handler,
    }
    defaults.update(kwargs)
    return RouteDefinition(**defaults)


# ── Mock Pydantic model ─────────────────────────────────────────────────


class FakeModel:
    """Simulates a Pydantic BaseModel for testing without requiring pydantic."""

    def __init__(self, *, name: str, age: int) -> None:
        self.name = name
        self.age = age

    @classmethod
    def model_validate(cls, data: dict) -> "FakeModel":
        return cls(name=data["name"], age=data["age"])

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FakeModel):
            return NotImplemented
        return self.name == other.name and self.age == other.age


class InvalidModel:
    """Simulates a Pydantic model that raises on validation."""

    @classmethod
    def model_validate(cls, data: dict) -> "InvalidModel":
        raise ValueError("Validation failed: missing required field 'email'")


# ── Tests ────────────────────────────────────────────────────────────────


class TestPydanticAutoValidation:
    def test_dict_auto_validated_to_pydantic_model(self) -> None:
        """When serializer returns dict and target has model_validate, auto-validate."""
        captured: list = []

        def handler(body: FakeModel) -> None:
            captured.append(body)

        serializer = MagicMock()
        serializer.decode.return_value = {"name": "Alice", "age": 30}

        route = _make_route(handler=handler, serializer_override=serializer)
        pipeline = HandlerPipeline()

        msg = _make_message()
        _wire_sync(msg)
        pipeline.process_sync(route, msg)

        assert len(captured) == 1
        assert isinstance(captured[0], FakeModel)
        assert captured[0].name == "Alice"
        assert captured[0].age == 30

    def test_already_model_passthrough(self) -> None:
        """When serializer returns an already-constructed model, pass through."""
        captured: list = []
        model_instance = FakeModel(name="Bob", age=25)

        def handler(body: FakeModel) -> None:
            captured.append(body)

        serializer = MagicMock()
        serializer.decode.return_value = model_instance  # Already a model, not a dict

        route = _make_route(handler=handler, serializer_override=serializer)
        pipeline = HandlerPipeline()

        msg = _make_message()
        _wire_sync(msg)
        pipeline.process_sync(route, msg)

        assert len(captured) == 1
        assert captured[0] is model_instance

    def test_invalid_data_raises_validation_error(self) -> None:
        """When model_validate raises, the error propagates (rejects message)."""

        def handler(body: InvalidModel) -> None:
            pass  # pragma: no cover

        serializer = MagicMock()
        serializer.decode.return_value = {"name": "Alice"}  # Missing 'email'

        route = _make_route(handler=handler, serializer_override=serializer)
        pipeline = HandlerPipeline()

        msg = _make_message()
        _wire_sync(msg)
        pipeline.process_sync(route, msg)

        # ValueError is classified as permanent -> reject
        assert msg._disposition == "rejected"

    def test_bytes_target_passthrough(self) -> None:
        """When target type is bytes, no deserialization or validation."""
        captured: list = []

        def handler(body: bytes) -> None:
            captured.append(body)

        route = _make_route(handler=handler)
        pipeline = HandlerPipeline()

        msg = _make_message(body=b"raw-data")
        _wire_sync(msg)
        pipeline.process_sync(route, msg)

        assert len(captured) == 1
        assert captured[0] == b"raw-data"

    def test_non_pydantic_dict_passthrough(self) -> None:
        """When target type is dict, decoded dict passes through without model_validate."""
        captured: list = []

        def handler(body: dict) -> None:
            captured.append(body)

        serializer = MagicMock()
        serializer.decode.return_value = {"key": "value"}

        route = _make_route(handler=handler, serializer_override=serializer)
        pipeline = HandlerPipeline()

        msg = _make_message()
        _wire_sync(msg)
        pipeline.process_sync(route, msg)

        assert len(captured) == 1
        assert captured[0] == {"key": "value"}

    def test_dict_target_type_no_auto_validate(self) -> None:
        """When target_type is dict (has no model_validate), decoded dict passes through."""
        captured: list = []

        def handler(body: dict) -> None:
            captured.append(body)

        serializer = MagicMock()
        serializer.decode.return_value = {"foo": "bar"}

        route = _make_route(handler=handler, serializer_override=serializer)
        pipeline = HandlerPipeline()

        msg = _make_message()
        _wire_sync(msg)
        pipeline.process_sync(route, msg)

        assert captured[0] == {"foo": "bar"}

    def test_no_serializer_returns_raw_body(self) -> None:
        """Without a serializer, body bytes are passed through as-is."""
        captured: list = []

        def handler(body: FakeModel) -> None:
            captured.append(body)

        route = _make_route(handler=handler, serializer_override=None)
        pipeline = HandlerPipeline()

        msg = _make_message(body=b'{"name": "test"}')
        _wire_sync(msg)
        pipeline.process_sync(route, msg)

        assert len(captured) == 1
        assert captured[0] == b'{"name": "test"}'
