import typing

import faust
import pytest
from faust.exceptions import KeyDecodeError, ValueDecodeError
from faust.utils import json
from mode.utils.mocks import Mock


class Case(typing.NamedTuple):
    payload: typing.Any
    key_type: typing.Any
    serializer: str
    expected: typing.Any


class User(faust.Record):
    id: str
    first_name: str
    last_name: str


class Account(faust.Record):
    id: str
    active: bool
    user: User


# Account1
USER1 = User('A2', 'George', 'Costanza')
ACCOUNT1 = Account(id='A1', active=True, user=USER1)
ACCOUNT1_JSON = ACCOUNT1.dumps(serializer='json')

# Account1 without blessed key
ACCOUNT1_UNBLESSED = ACCOUNT1.to_representation()
ACCOUNT1_UNBLESSED.pop('__faust')
ACCOUNT1_UNBLESSED_JSON = json.dumps(ACCOUNT1_UNBLESSED)

# Account1 with extra fields
ACCOUNT1_EXTRA_FIELDS = ACCOUNT1.to_representation()
ACCOUNT1_EXTRA_FIELDS.update(
    join_date='12321321312',
    foo={'a': 'A', 'b': 'B'},
    bar=[1, 2, 3, 4],
)
ACCOUNT1_EXTRA_FIELDS_JSON = json.dumps(ACCOUNT1_EXTRA_FIELDS)

# Account2
USER2 = User('B2', 'Elaine', 'Benes')
ACCOUNT2 = Account(id='B1', active=True, user=USER2)
ACCOUNT2_JSON = ACCOUNT2.dumps(serializer='json')

# Json data not made by Faust.
NONFAUST = {
    'str': 'str',
    'int': 1,
    'float': 0.30,
    'lst': [1, 2, 3],
    'dct': {'a': 1, 'b': 2},
}

A_BYTE_STR = b'the quick brown fox'
A_STR_STR = 'the quick brown fox'

VALUE_TESTS = [
    # ### value_type=None, serializer='json'
    #   autodetects blessed records -> Account
    Case(ACCOUNT1_JSON, None, 'json', ACCOUNT1),
    Case(ACCOUNT1_EXTRA_FIELDS_JSON, None, 'json', ACCOUNT1),
    Case(ACCOUNT2_JSON, None, 'json', ACCOUNT2),

    #   but unblessed record payload -> mapping
    Case(ACCOUNT1_UNBLESSED_JSON, None, 'json', ACCOUNT1_UNBLESSED),

    #   source json dict mapping -> mapping
    Case(json.dumps(NONFAUST), None, 'json', NONFAUST),

    #   source json string payload -> str
    Case(json.dumps(A_STR_STR), None, 'json', A_STR_STR),

    # ### value_type=None, serializer='raw'
    #   source unicode payload -> bytes
    Case(A_STR_STR, None, 'raw', A_BYTE_STR),

    #   source bytes payload -> bytes
    Case(A_BYTE_STR, None, 'raw', A_BYTE_STR),

    # ### value_type=bytes, serializer='json'
    #   source json str -> bytes
    Case(json.dumps(A_STR_STR), bytes, 'json', A_BYTE_STR),

    # ### value_type=str, serializer='json'
    #   source json str -> str
    Case(json.dumps(A_STR_STR), str, 'json', A_STR_STR),

    # ###  value_type=bytes, serializer='raw'
    #  source bytes -> bytes
    Case(A_BYTE_STR, bytes, 'raw', A_BYTE_STR),

    # ### value_type=str, serializer='raw'
    #   source str gives str
    Case(A_STR_STR, str, 'raw', A_STR_STR),

    # ### value_type=Account, serializer='json'
    #   source Account json -> Account
    Case(ACCOUNT1_JSON, Account, 'json', ACCOUNT1),
    Case(ACCOUNT1_EXTRA_FIELDS_JSON, Account, 'json', ACCOUNT1),

    # source non blessed json -> Account
    Case(ACCOUNT1_UNBLESSED_JSON, Account, 'json', ACCOUNT1),

    # source blessed User json -> User (!!!)
    # value_type is None so it accepts any type
    Case(USER1.dumps(serializer='json'), None, 'json', USER1),

    # key/value=None
    Case(None, None, 'json', None),
]


@pytest.mark.parametrize('payload,typ,serializer,expected', VALUE_TESTS)
def test_loads_key(payload, typ, serializer, expected, *, app):
    assert app.serializers.loads_key(
        typ, payload, serializer=serializer) == expected


def test_loads_key__expected_model_received_None(*, app):
    with pytest.raises(KeyDecodeError):
        app.serializers.loads_key(Account, None, serializer='json')


def test_loads_key__propagates_MemoryError(*, app):
    app.serializers._loads = Mock(name='_loads')
    app.serializers._loads.side_effect = MemoryError()
    with pytest.raises(MemoryError):
        app.serializers.loads_key(Account, ACCOUNT1_JSON, serializer='json')


def test_loads_value__propagates_MemoryError(*, app):
    app.serializers._loads = Mock(name='_loads')
    app.serializers._loads.side_effect = MemoryError()
    with pytest.raises(MemoryError):
        app.serializers.loads_value(Account, ACCOUNT1_JSON, serializer='json')


def test_loads_value__expected_model_received_None(*, app):
    with pytest.raises(ValueDecodeError):
        app.serializers.loads_value(Account, None, serializer='json')


@pytest.mark.parametrize('payload,typ,serializer,expected', VALUE_TESTS)
def test_loads_value(payload, typ, serializer, expected, *, app):
    assert app.serializers.loads_value(
        typ, payload, serializer=serializer) == expected


def test_loads_value_missing_key_raises_error(*, app):
    account = ACCOUNT1.to_representation()
    account.pop('active')
    with pytest.raises(ValueDecodeError):
        app.serializers.loads_value(
            Account, json.dumps(account), serializer='json')


def test_loads_key_missing_key_raises_error(*, app):
    account = ACCOUNT1.to_representation()
    account.pop('active')
    with pytest.raises(KeyDecodeError):
        app.serializers.loads_key(
            Account, json.dumps(account), serializer='json')


def test_dumps_value__bytes(*, app):
    assert app.serializers.dumps_value(
        bytes, b'foo', serializer='json') == b'foo'


@pytest.mark.parametrize('typ,alt,expected', [
    (str, (), 'raw'),
    (bytes, (), 'raw'),
    (str, ('json',), 'json'),
    (bytes, ('json',), 'json'),
    (None, (), None),
])
def test_serializer_type(typ, alt, expected, *, app):
    assert app.serializers._serializer(typ, *alt) == expected
