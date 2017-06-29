from datetime import date, datetime, timezone
from decimal import Decimal, InvalidOperation
from uuid import uuid4
from faust.utils.json import JSONEncoder, str_to_decimal
from hypothesis import assume, given, reject
from hypothesis.strategies import decimals, text
import pytest


@given(text())
def test_str_to_decimal_text_values(x):
    try:
        str_to_decimal(x)
    except InvalidOperation:
        reject()
    except ValueError:
        try:
            assume(not Decimal(x).is_finite())
        except ValueError:
            pass
        raise


@given(decimals())
def test_str_to_decimal_decimals(x):
    assume(x.is_finite())
    assert str_to_decimal(str(x)) == x


def test_str_to_decimal_None():
    assert str_to_decimal(None) is None


def test_str():
    d1 = Decimal('3.3333433434343434343434343434343')
    assert str_to_decimal(str(d1)) == d1


def test_maxlen():
    s = '3.' + '34' * 1000
    with pytest.raises(ValueError):
        str_to_decimal(s)


def test_NaN():
    with pytest.raises(ValueError):
        str_to_decimal('NaN')


def test_Inf():
    with pytest.raises(ValueError):
        str_to_decimal('Inf')


def test_negative_Inf():
    with pytest.raises(ValueError):
        str_to_decimal('-Inf')


def test_sNaN():
    with pytest.raises(ValueError):
        str_to_decimal('sNaN')


def test_JSONEncoder():
    encoder = JSONEncoder()
    assert encoder.default(date(2016, 3, 2))
    assert encoder.default(datetime.utcnow())
    assert encoder.default(datetime.now(timezone.utc))
    assert encoder.default(uuid4())
    with pytest.raises(TypeError):
        encoder.default(object())
