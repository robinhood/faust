import pytest
from faust.exceptions import SecurityError
from faust.models.tags import Secret, Sensitive


class test_Sensitive:

    @pytest.fixture
    def typ(self):
        return Sensitive[str]

    @pytest.fixture
    def v(self, *, typ):
        return typ('value')

    def test_repr(self, *, v):
        assert repr(v)
        assert 'value' not in repr(v)

    def test_str(self, *, v):
        with pytest.raises(SecurityError):
            str(v)

    def test_nested(self, *, v, typ):
        with pytest.raises(SecurityError):
            typ(v)


class test_Secret(test_Sensitive):

    @pytest.fixture
    def typ(self):
        return Secret[str]

    def test_str(self, *, v):
        assert str(v) == v.mask
