"""Tests for serialization/pipeline.py — two-stage serialization pipeline."""

from __future__ import annotations

import json
from dataclasses import dataclass
from unittest.mock import MagicMock

from rabbitkit.serialization.pipeline import (
    DataclassDecoder,
    JsonParser,
    PydanticDecoder,
    RawDecoder,
    SerializationPipeline,
)

# ── Mock Pydantic model ─────────────────────────────────────────────────


class FakePydanticModel:
    """Simulates a Pydantic BaseModel."""

    def __init__(self, *, name: str, value: int) -> None:
        self.name = name
        self.value = value

    @classmethod
    def model_validate(cls, data: dict) -> FakePydanticModel:
        return cls(name=data["name"], value=data["value"])

    def model_dump(self) -> dict:
        return {"name": self.name, "value": self.value}

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, FakePydanticModel):
            return NotImplemented
        return self.name == other.name and self.value == other.value


# ── JsonParser ───────────────────────────────────────────────────────────


class TestJsonParser:
    def test_parse(self) -> None:
        parser = JsonParser()
        result = parser.parse(b'{"key": "value"}')
        assert result == {"key": "value"}

    def test_parse_list(self) -> None:
        parser = JsonParser()
        result = parser.parse(b"[1, 2, 3]")
        assert result == [1, 2, 3]

    def test_serialize(self) -> None:
        parser = JsonParser()
        result = parser.serialize({"key": "value"})
        assert json.loads(result) == {"key": "value"}

    def test_content_type(self) -> None:
        parser = JsonParser()
        assert parser.content_type == "application/json"

    def test_parse_with_content_type(self) -> None:
        parser = JsonParser()
        result = parser.parse(b'{"a": 1}', content_type="application/json")
        assert result == {"a": 1}


# ── PydanticDecoder ──────────────────────────────────────────────────────


class TestPydanticDecoder:
    def test_decode_dict_to_model(self) -> None:
        decoder = PydanticDecoder()
        result = decoder.decode({"name": "test", "value": 42}, FakePydanticModel)
        assert isinstance(result, FakePydanticModel)
        assert result.name == "test"
        assert result.value == 42

    def test_decode_non_dict_passthrough(self) -> None:
        decoder = PydanticDecoder()
        result = decoder.decode("plain-string", FakePydanticModel)
        assert result == "plain-string"

    def test_decode_non_pydantic_type_passthrough(self) -> None:
        decoder = PydanticDecoder()
        data = {"key": "value"}
        result = decoder.decode(data, dict)
        assert result == {"key": "value"}

    def test_encode_model(self) -> None:
        decoder = PydanticDecoder()
        model = FakePydanticModel(name="test", value=42)
        result = decoder.encode(model)
        assert result == {"name": "test", "value": 42}

    def test_encode_non_model_passthrough(self) -> None:
        decoder = PydanticDecoder()
        result = decoder.encode({"key": "value"})
        assert result == {"key": "value"}


# ── DataclassDecoder ─────────────────────────────────────────────────────


class TestDataclassDecoder:
    def test_decode_dict_to_dataclass(self) -> None:
        @dataclass
        class Order:
            id: int
            name: str

        decoder = DataclassDecoder()
        result = decoder.decode({"id": 1, "name": "test"}, Order)
        assert isinstance(result, Order)
        assert result.id == 1
        assert result.name == "test"

    def test_decode_non_dict_passthrough(self) -> None:
        @dataclass
        class Order:
            id: int
            name: str

        decoder = DataclassDecoder()
        result = decoder.decode("not-a-dict", Order)
        assert result == "not-a-dict"

    def test_decode_non_dataclass_passthrough(self) -> None:
        decoder = DataclassDecoder()
        data = {"id": 1}
        result = decoder.decode(data, dict)
        assert result == {"id": 1}

    def test_encode_dataclass(self) -> None:
        @dataclass
        class Order:
            id: int
            name: str

        decoder = DataclassDecoder()
        result = decoder.encode(Order(id=1, name="test"))
        assert result == {"id": 1, "name": "test"}

    def test_encode_non_dataclass_passthrough(self) -> None:
        decoder = DataclassDecoder()
        result = decoder.encode({"key": "value"})
        assert result == {"key": "value"}

    def test_encode_dataclass_type_passthrough(self) -> None:
        """Encoding a dataclass class (not instance) passes through."""

        @dataclass
        class Order:
            id: int
            name: str

        decoder = DataclassDecoder()
        result = decoder.encode(Order)
        assert result is Order


# ── RawDecoder ───────────────────────────────────────────────────────────


class TestRawDecoder:
    def test_decode_passthrough(self) -> None:
        decoder = RawDecoder()
        data = {"key": "value"}
        result = decoder.decode(data, dict)
        assert result is data

    def test_encode_passthrough(self) -> None:
        decoder = RawDecoder()
        data = {"key": "value"}
        result = decoder.encode(data)
        assert result is data


# ── SerializationPipeline ────────────────────────────────────────────────


class TestSerializationPipeline:
    def test_round_trip_json_pydantic(self) -> None:
        pipeline = SerializationPipeline(JsonParser(), PydanticDecoder())
        model = FakePydanticModel(name="alice", value=99)

        encoded = pipeline.encode(model)
        assert isinstance(encoded, bytes)
        parsed = json.loads(encoded)
        assert parsed == {"name": "alice", "value": 99}

        decoded = pipeline.decode(encoded, FakePydanticModel)
        assert isinstance(decoded, FakePydanticModel)
        assert decoded == model

    def test_round_trip_json_dataclass(self) -> None:
        @dataclass
        class Event:
            type: str
            count: int

        pipeline = SerializationPipeline(JsonParser(), DataclassDecoder())
        original = Event(type="click", count=5)

        encoded = pipeline.encode(original)
        decoded = pipeline.decode(encoded, Event)
        assert isinstance(decoded, Event)
        assert decoded == original

    def test_round_trip_json_raw(self) -> None:
        pipeline = SerializationPipeline(JsonParser(), RawDecoder())
        original = {"hello": "world"}

        encoded = pipeline.encode(original)
        decoded = pipeline.decode(encoded, dict)
        assert decoded == original

    def test_content_type_from_parser(self) -> None:
        pipeline = SerializationPipeline(JsonParser(), RawDecoder())
        assert pipeline.content_type == "application/json"

    def test_encode_decode_with_mock_parser_decoder(self) -> None:
        """Verify pipeline wiring: parser.serialize(decoder.encode(data)) and reverse."""
        parser = MagicMock()
        parser.serialize.return_value = b"serialized"
        parser.parse.return_value = {"intermediate": True}
        parser.content_type = "custom/type"

        decoder = MagicMock()
        decoder.encode.return_value = {"intermediate": True}
        decoder.decode.return_value = "final-value"

        pipeline = SerializationPipeline(parser, decoder)

        # Encode
        result = pipeline.encode("input-data")
        decoder.encode.assert_called_once_with("input-data")
        parser.serialize.assert_called_once_with({"intermediate": True})
        assert result == b"serialized"

        # Decode
        result = pipeline.decode(b"raw-bytes", str)
        parser.parse.assert_called_once_with(b"raw-bytes")
        decoder.decode.assert_called_once_with({"intermediate": True}, str)
        assert result == "final-value"

        # Content type
        assert pipeline.content_type == "custom/type"


class TestJsonParserDefault:
    def test_coerce_unknown_to_str(self) -> None:
        """_default returns str(data) when coerce_unknown_to_str=True."""
        parser = JsonParser(coerce_unknown_to_str=True)

        class NotSerializable:
            def __repr__(self) -> str:
                return "NotSerializable()"

        result = parser.serialize(NotSerializable())
        assert b"NotSerializable" in result

    def test_no_coerce_raises_type_error(self) -> None:
        """_default raises TypeError when coerce_unknown_to_str=False (default)."""
        parser = JsonParser()

        class NotSerializable:
            pass

        import pytest
        with pytest.raises(TypeError):
            parser.serialize(NotSerializable())
