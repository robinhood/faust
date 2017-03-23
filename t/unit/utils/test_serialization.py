import base64
import json as _json
import pytest
from typing import Mapping
from hypothesis import given
from hypothesis.strategies import binary, dictionaries, text
from faust.utils.compat import want_str
from faust.utils.serialization import (
    Serializer, get_serializer, loads, dumps, json, binary as _binary,
)

DATA = {'a': 1, 'b': 'string'}


def test_interface():
    s = Serializer()
    with pytest.raises(NotImplementedError):
        s._loads(b'foo')
    with pytest.raises(NotImplementedError):
        s.dumps(10)
    assert s.__or__(1) is NotImplemented


@pytest.mark.parametrize('serializer', ['json', 'pickle'])
def test_json_subset(serializer: str) -> None:
    assert loads(serializer, dumps(serializer, DATA)) == DATA


@given(binary())
def test_binary(input: bytes) -> None:
    assert loads('binary', dumps('binary', input)) == input


@given(dictionaries(text(), text()))
def test_combinators(input: Mapping[str, str]) -> None:
    s = json() | _binary()
    assert repr(s).replace("u'", "'") == "json() | binary()"

    d = s.dumps(input)
    assert isinstance(d, bytes)
    assert _json.loads(want_str(base64.b64decode(d))) == input


def test_get_serializer():
    assert get_serializer('json|binary')
    assert get_serializer(Serializer) is Serializer
