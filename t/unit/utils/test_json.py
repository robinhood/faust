import enum
from datetime import date, datetime, timezone
from decimal import Decimal
from uuid import uuid4
from faust.utils.json import JSONEncoder, str_to_decimal
from hypothesis import assume, given
from hypothesis.strategies import decimals
import pytest


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


class Flags(enum.Enum):
    X = 'Xval'
    Y = 'Yval'


class CanJson:

    def __json__(self):
        return 'yes'


def test_JSONEncoder():
    encoder = JSONEncoder()
    assert encoder.default(date(2016, 3, 2))
    assert encoder.default(datetime.utcnow())
    assert encoder.default(datetime.now(timezone.utc))
    assert encoder.default(uuid4())
    assert encoder.default(Flags.X) == 'Xval'
    assert encoder.default(Flags.Y) == 'Yval'
    assert encoder.default({1, 2, 3}) == [1, 2, 3]
    assert encoder.default(CanJson()) == 'yes'
    with pytest.raises(TypeError):
        encoder.default(object())
