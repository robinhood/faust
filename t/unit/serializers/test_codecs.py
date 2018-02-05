import base64
from typing import Mapping
from mode.utils.compat import want_str
from faust.serializers.codecs import (
    Codec, binary as _binary, dumps, get_codec, json, loads,
)
from faust.utils import json as _json
from hypothesis import given
from hypothesis.strategies import binary, dictionaries, text
import pytest

DATA = {'a': 1, 'b': 'string'}


def test_interface():
    s = Codec()
    with pytest.raises(NotImplementedError):
        s._loads(b'foo')
    with pytest.raises(NotImplementedError):
        s.dumps(10)
    assert s.__or__(1) is NotImplemented


@pytest.mark.parametrize('codec', ['json', 'pickle'])
def test_json_subset(codec: str) -> None:
    assert loads(codec, dumps(codec, DATA)) == DATA


@given(binary())
def test_binary(input: bytes) -> None:
    assert loads('binary', dumps('binary', input)) == input


@given(dictionaries(text(), text()))
def test_combinators(input: Mapping[str, str]) -> None:
    s = json() | _binary()
    assert repr(s).replace('u\'', '\'') == 'json() | binary()'

    d = s.dumps(input)
    assert isinstance(d, bytes)
    assert _json.loads(want_str(base64.b64decode(d))) == input


def test_get_codec():
    assert get_codec('json|binary')
    assert get_codec(Codec) is Codec
