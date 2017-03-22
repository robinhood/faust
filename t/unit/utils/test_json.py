import math
import pytest
from decimal import Decimal, InvalidOperation
from six import text_type
from hypothesis import assume, given, reject
from hypothesis.strategies import decimals, text
from faust.utils.json import str_to_decimal


@given(text())
def test_str_to_decimal_text_values(x):
    try:
        str_to_decimal(x)
    except InvalidOperation:
        reject()
    except ValueError:
        assume(math.isnan(Decimal(x)))
        assume(math.isinf(Decimal(x)))
        raise


@given(decimals())
def test_str_to_decimal_decimals(x):
    assume(not math.isnan(x))
    assume(not math.isinf(x))
    assert str_to_decimal(str(x)) == x


def test_str():
    d1 = Decimal('3.3333433434343434343434343434343')
    assert str_to_decimal(text_type(d1)) == d1


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
