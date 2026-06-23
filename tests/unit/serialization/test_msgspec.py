"""Tests for serialization/msgspec.py — MsgspecSerializer."""

from __future__ import annotations

import pytest

# msgspec is optional — skip all tests if not installed
msgspec = pytest.importorskip("msgspec")

from rabbitkit.serialization.base import Serializer  # noqa: E402
from rabbitkit.serialization.msgspec import MsgspecSerializer  # noqa: E402


class SampleStruct(msgspec.Struct):
    id: int
    name: str


# ── protocol compliance ─────────────────────────────────────────────────


class TestProtocolCompliance:
    def test_satisfies_serializer_protocol(self) -> None:
        s = MsgspecSerializer()
        assert isinstance(s, Serializer)

    def test_content_type(self) -> None:
        s = MsgspecSerializer()
        assert s.content_type == "application/json"


# ── encode ───────────────────────────────────────────────────────────────


class TestEncode:
    def test_encode_dict(self) -> None:
        s = MsgspecSerializer()
        result = s.encode({"key": "value"})
        decoded = msgspec.json.decode(result)
        assert decoded == {"key": "value"}

    def test_encode_struct(self) -> None:
        s = MsgspecSerializer()
        result = s.encode(SampleStruct(id=1, name="test"))
        decoded = msgspec.json.decode(result, type=SampleStruct)
        assert decoded.id == 1
        assert decoded.name == "test"

    def test_encode_bytes_passthrough(self) -> None:
        s = MsgspecSerializer()
        data = b"raw"
        assert s.encode(data) is data

    def test_encode_string(self) -> None:
        s = MsgspecSerializer()
        result = s.encode("hello")
        assert result == b"hello"


# ── decode ───────────────────────────────────────────────────────────────


class TestDecode:
    def test_decode_struct(self) -> None:
        s = MsgspecSerializer()
        data = msgspec.json.encode(SampleStruct(id=1, name="test"))
        result = s.decode(data, SampleStruct)
        assert isinstance(result, SampleStruct)
        assert result.id == 1
        assert result.name == "test"

    def test_decode_to_bytes(self) -> None:
        s = MsgspecSerializer()
        data = b"raw"
        assert s.decode(data, bytes) is data

    def test_decode_to_str(self) -> None:
        s = MsgspecSerializer()
        assert s.decode(b"hello", str) == "hello"

    def test_decode_dict(self) -> None:
        s = MsgspecSerializer()
        data = msgspec.json.encode({"key": "value"})
        result = s.decode(data, dict)
        assert result == {"key": "value"}


# ── round-trip ───────────────────────────────────────────────────────────


class TestRoundTrip:
    def test_struct_round_trip(self) -> None:
        s = MsgspecSerializer()
        original = SampleStruct(id=42, name="round-trip")
        encoded = s.encode(original)
        decoded = s.decode(encoded, SampleStruct)
        assert decoded == original
