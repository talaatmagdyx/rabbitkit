"""Property-based round-trip tests for the built-in serializers (L17).

For every generated JSON-compatible value, ``decode(encode(value), target_type)``
must recover the original value exactly. Hand-picked unit-test examples
under-sample edge cases (empty containers, unicode, large/negative numbers,
deep nesting) that hypothesis's shrinking search finds reliably.
"""

from __future__ import annotations

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from rabbitkit.serialization.json import JSONSerializer

# JSON-compatible scalars. NaN/Infinity excluded: not valid JSON, and NaN
# breaks equality-based round-trip assertions (NaN != NaN) even when the
# serializer's own NaN handling is self-consistent.
_json_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**63), max_value=2**63 - 1),
    st.floats(allow_nan=False, allow_infinity=False, width=64),
    st.text(),
)

_json_value = st.recursive(
    _json_scalars,
    lambda children: st.one_of(
        st.lists(children, max_size=8),
        st.dictionaries(st.text(), children, max_size=8),
    ),
    max_leaves=30,
)


class TestJSONSerializerRoundtrip:
    @given(value=st.dictionaries(st.text(), _json_value, max_size=10))
    @settings(max_examples=200)
    def test_dict_roundtrips(self, value: dict) -> None:
        serializer = JSONSerializer()
        encoded = serializer.encode(value)
        decoded = serializer.decode(encoded, dict)
        assert decoded == value

    @given(value=st.lists(_json_value, max_size=10))
    @settings(max_examples=200)
    def test_list_roundtrips(self, value: list) -> None:
        serializer = JSONSerializer()
        encoded = serializer.encode(value)
        decoded = serializer.decode(encoded, list)
        assert decoded == value

    @given(value=st.text())
    @settings(max_examples=200)
    def test_str_roundtrips(self, value: str) -> None:
        serializer = JSONSerializer()
        encoded = serializer.encode(value)
        decoded = serializer.decode(encoded, str)
        assert decoded == value

    @given(value=st.binary(max_size=256))
    @settings(max_examples=100)
    def test_bytes_passthrough_roundtrips(self, value: bytes) -> None:
        """bytes bypass JSON entirely (pass-through) -- must be lossless
        for arbitrary binary content, not just valid-UTF-8 subsets."""
        serializer = JSONSerializer()
        encoded = serializer.encode(value)
        decoded = serializer.decode(encoded, bytes)
        assert decoded == value


class TestMsgspecSerializerRoundtrip:
    @pytest.fixture(autouse=True)
    def _check_msgspec(self) -> None:
        pytest.importorskip("msgspec")

    @given(value=st.dictionaries(st.text(), _json_value, max_size=10))
    @settings(max_examples=200)
    def test_dict_roundtrips(self, value: dict) -> None:
        from rabbitkit.serialization.msgspec import MsgspecSerializer

        serializer = MsgspecSerializer()
        encoded = serializer.encode(value)
        decoded = serializer.decode(encoded, dict)
        assert decoded == value

    @given(value=st.lists(_json_value, max_size=10))
    @settings(max_examples=200)
    def test_list_roundtrips(self, value: list) -> None:
        from rabbitkit.serialization.msgspec import MsgspecSerializer

        serializer = MsgspecSerializer()
        encoded = serializer.encode(value)
        decoded = serializer.decode(encoded, list)
        assert decoded == value

    @given(value=st.text())
    @settings(max_examples=200)
    def test_str_roundtrips(self, value: str) -> None:
        from rabbitkit.serialization.msgspec import MsgspecSerializer

        serializer = MsgspecSerializer()
        encoded = serializer.encode(value)
        decoded = serializer.decode(encoded, str)
        assert decoded == value
