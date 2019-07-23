from decimal import Decimal
import pytest
from faust.exceptions import ValidationError
from faust.models.fields import BytesField, DecimalField, FieldDescriptor


class test_FieldDescriptor:

    def test_validate(self):
        f = FieldDescriptor()
        assert list(f.validate('foo')) == []


class test_DecimalField:

    def test_init_options(self):
        assert DecimalField(max_digits=3).max_digits == 3
        assert DecimalField(max_decimal_places=4).max_decimal_places == 4

        f = DecimalField(max_digits=3, max_decimal_places=4)
        f2 = f.clone()
        assert f2.max_digits == 3
        assert f2.max_decimal_places == 4

        f3 = DecimalField()
        assert f3.max_digits is None
        assert f3.max_decimal_places is None
        f4 = f3.clone()
        assert f4.max_digits is None
        assert f4.max_decimal_places is None

    @pytest.mark.parametrize('value', [
        Decimal('Inf'),
        Decimal('NaN'),
        Decimal('sNaN'),
    ])
    def test_infinite(self, value):
        f = DecimalField(coerce=True, field='foo')
        with pytest.raises(ValidationError):
            raise next(f.validate(value))

    @pytest.mark.parametrize('value,places,digits', [
        (Decimal(4.1), 100, 2),
        (Decimal(4.1), 100, 2),
        (Decimal(4.1), None, 2),
        (Decimal(4.12), 100, None),
        (Decimal(4.123), 100, None),
        (4.1234, 100, 2),
        (Decimal(4.1234), 100, 2),
        (Decimal(123456612341.1234), 100, 100),
    ])
    def test_max_decimal_places__good(self, value, places, digits):
        f = DecimalField(
            max_decimal_places=places,
            max_digits=digits,
            coerce=True,
            field='foo',
        )
        d: Decimal = f.prepare_value(value)
        for error in f.validate(d):
            raise error

    @pytest.mark.parametrize('value', [
        Decimal(1.12412421421),
        Decimal(1.12345),
        Decimal(123456788.12345),
    ])
    def test_max_decimal_places__bad(self, value):
        f = DecimalField(max_decimal_places=4, coerce=True, field='foo')
        with pytest.raises(ValidationError):
            raise next(f.validate(value))

    @pytest.mark.parametrize('value', [
        Decimal(12345.12412421421),
        Decimal(123456.12345),
        Decimal(123456788.12345),
    ])
    def test_max_digits__bad(self, value):
        f = DecimalField(max_digits=4, coerce=True, field='foo')
        with pytest.raises(ValidationError):
            raise next(f.validate(value))


class test_BytesField:

    def test_init_options(self):
        assert BytesField(encoding='latin1').encoding == 'latin1'
        assert BytesField(errors='replace').errors == 'replace'

        f = BytesField(encoding='latin1', errors='replace')
        f2 = f.clone()
        assert f2.encoding == 'latin1'
        assert f2.errors == 'replace'

        f3 = BytesField()
        assert f3.encoding == 'utf-8'
        assert f3.errors == 'strict'
        f4 = f3.clone()
        assert f4.encoding == 'utf-8'
        assert f4.errors == 'strict'

    @pytest.mark.parametrize('value,coerce,trim,expected_result', [
        ('foo', True, False, b'foo'),
        (b'foo', True, False, b'foo'),
        ('foo', False, False, 'foo'),
        ('  fo o   ', True, True, b'fo o'),
        (b'  fo o   ', True, True, b'fo o'),
        ('  fo o   ', True, False, b'  fo o   '),
    ])
    def test_prepare_value(self, value, coerce, trim, expected_result):
        f = BytesField(coerce=coerce, trim_whitespace=trim)
        assert f.prepare_value(value) == expected_result