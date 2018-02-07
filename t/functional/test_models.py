from datetime import datetime
from typing import ClassVar, Dict, List, Mapping, Optional, Set, Tuple
from faust import Record
from faust.utils import json
import pytest


class Account(Record):
    id: str
    name: str
    active: bool = True


class AccountList(Record):
    accounts: List[Account]


class AccountSet(Record):
    accounts: Set[Account]


class AccountMap(Record):
    accounts: Dict[str, Account]


class FREFAccountList(Record):
    accounts: 'List[Account]'


class FREFAccountSet(Record):
    accounts: 'Set[Account]'


class FREFAccountMap(Record):
    accounts: Dict[str, 'Account']
    something: 'ClassVar[bool]' = True


class User(Record):
    id: str
    username: str
    account: Account


class Date(Record, isodates=True):
    date: datetime


class ListOfDate(Record, isodates=True):
    dates: List[datetime]


class OptionalListOfDate(Record, isodates=True):
    dates: List[datetime] = None


class OptionalListOfDate2(Record, isodates=True):
    dates: Optional[List[datetime]]


class TupleOfDate(Record, isodates=True):
    dates: Tuple[datetime]


class SetOfDate(Record, isodates=True):
    dates: Set[datetime]


class MapOfDate(Record, isodates=True):
    dates: Mapping[int, datetime]


def test_parameters():
    account = Account('id', 'name', True)
    assert account.id == 'id'
    assert account.name == 'name'
    assert account.active

    account2 = Account('id', 'name', active=False)
    assert account2.id == 'id'
    assert account2.name == 'name'
    assert not account2.active

    class Account3(Account):
        foo: int = None

    account3 = Account3('id', 'name', False, 'foo')
    assert account3.id == 'id'
    assert account3.name == 'name'
    assert not account3.active
    assert account3.foo == 'foo'

    with pytest.raises(AttributeError):
        account2.foo


def test_paramters_with_custom_init():

    class Point(Record, include_metadata=False):
        x: int
        y: int

        def __init__(self, x, y, **kwargs):
            self.x = x
            self.y = y

    p = Point(30, 10)
    assert p.x == 30
    assert p.y == 10

    payload = p.dumps(serializer='json')
    assert payload == b'{"x": 30, "y": 10}'

    data = json.loads(payload)
    p2 = Point.from_data(data)
    assert p2.x == 30
    assert p2.y == 10


def test_parameters_with_custom_init_and_super():

    class Point(Record, include_metadata=False):
        x: int
        y: int

        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.z = self.x + self.y

    p = Point(30, 10)
    assert p.x == 30
    assert p.y == 10
    assert p.z == 40

    payload = p.dumps(serializer='json')
    assert payload == b'{"x": 30, "y": 10}'

    data = json.loads(payload)
    p2 = Point.from_data(data)
    assert p2.x == 30
    assert p2.y == 10
    assert p2.z == 40


def test_isodates():
    n1 = datetime.utcnow()
    assert Date.loads(Date(date=n1).dumps()).date == n1
    n2 = datetime.utcnow()
    assert ListOfDate.loads(ListOfDate(
        dates=[n1, n2]).dumps()).dates == [n1, n2]
    assert OptionalListOfDate.loads(OptionalListOfDate(
        dates=None).dumps()).dates is None
    assert OptionalListOfDate.loads(OptionalListOfDate(
        dates=[n2, n1]).dumps()).dates == [n2, n1]
    assert OptionalListOfDate2.loads(OptionalListOfDate2(
        dates=None).dumps()).dates is None
    assert OptionalListOfDate2.loads(OptionalListOfDate2(
        dates=[n1, n2]).dumps()).dates == [n1, n2]
    assert TupleOfDate.loads(TupleOfDate(
        dates=(n1, n2)).dumps()).dates == (n1, n2)
    assert TupleOfDate.loads(TupleOfDate(
        dates=(n2,)).dumps()).dates == (n2,)
    assert SetOfDate.loads(SetOfDate(
        dates={n1, n2}).dumps()).dates == {n1, n2}
    assert MapOfDate.loads(MapOfDate(
        dates={101: n1, 202: n2}).dumps()).dates == {101: n1, 202: n2}


def test_constructor():
    with pytest.raises(TypeError):
        Account(id='123')
    with pytest.raises(TypeError):
        Account(name='foo')
    with pytest.raises(TypeError):
        Account(unknown_argument=303)
    account = Account(id='123', name='foo')
    assert account.id == '123'
    assert account.name == 'foo'
    assert account.active
    assert not Account(id='123', name='foo', active=False).active


def test_submodels():
    a1 = Account(id='123', name='foo', active=True)
    a2 = Account(id='456', name='bar', active=False)
    a3 = Account(id='789', name='baz', active=True)

    assert AccountList.loads(AccountList(
        accounts=[a1, a2, a3]).dumps()).accounts == [a1, a2, a3]
    assert AccountSet.loads(AccountSet(
        accounts={a1, a2, a3}).dumps()).accounts == {a1, a2, a3}
    assert AccountMap.loads(AccountMap(
        accounts={'a': a1, 'b': a2, 'c': a3}).dumps()).accounts == {
        'a': a1,
        'b': a2,
        'c': a3,
    }


def test_submodels_forward_reference():
    a1 = Account(id='123', name='foo', active=True)
    a2 = Account(id='456', name='bar', active=False)
    a3 = Account(id='789', name='baz', active=True)

    assert AccountList.loads(FREFAccountList(
        accounts=[a1, a2, a3]).dumps()).accounts == [a1, a2, a3]
    assert AccountSet.loads(FREFAccountSet(
        accounts={a1, a2, a3}).dumps()).accounts == {a1, a2, a3}
    assert AccountMap.loads(FREFAccountMap(
        accounts={'a': a1, 'b': a2, 'c': a3}).dumps()).accounts == {
        'a': a1,
        'b': a2,
        'c': a3,
    }

def test_derive():
    a1 = Account(id='123', name='foo', active=True)
    b1 = Account(id='456', name='bar', active=False)
    c1 = Account(id='789', name='baz', active=True)

    assert a1.active
    a2 = a1.derive(active=False)
    assert a2.id == '123'
    assert a2.name == 'foo'
    assert not a2.active

    c2 = a1.derive(b1, c1, name='xuzzy')
    assert c2.id == '789'
    assert c2.name == 'xuzzy'
    assert c2.active

    b2 = b1.derive(active=True)
    assert b2.active
    assert b2.id == '456'
    assert b2.name == 'bar'


def test_classvar_is_not_a_field():

    class PP(Record):
        x: int
        y: int
        z: ClassVar[int] = 3

    p = PP(10, 3)
    assert 'z' not in repr(p)
    assert p.asdict() == {'x': 10, 'y': 3}
    assert p.z == 3

    with pytest.raises(TypeError):
        PP(10, 3, z=300)

    p.z = 4
    assert p.z == 4


def test_constructor_from_data():
    with pytest.raises(TypeError):
        Account.from_data({'id': '123'})
    with pytest.raises(TypeError):
        Account.from_data({'name': 'foo'})
    with pytest.raises(TypeError):
        Account.from_data({'unknown_argument': 303})
    account = Account.from_data({'id': '123', 'name': 'foo'})
    assert account.id == '123'
    assert account.name == 'foo'
    assert account.active
    assert not Account.from_data(
        {'id': '123', 'name': 'foo', 'active': False}).active


@pytest.mark.parametrize('a,b', [
    (User(id=1, username=2, account=Account(id=1, name=2)),
     User(id=1, username=2, account=Account(id=2, name=2))),
    (User(id=1, username=2, account=Account(id=1, name=2)),
     User(id=2, username=2, account=Account(id=1, name=2))),
    (User(id=1, username=2, account=Account(id=1, name=2)),
     User(id=1, username=3, account=Account(id=1, name=2))),
    (User(id=1, username=2, account=Account(id=1, name=2)),
     User(id=1, username=2, account=Account(id=1, name=3))),
    (User(id=1,
          username=2,
          account=Account(id=1, name=2, active=False)),
     User(id=1,
          username=2,
          account=Account(id=1, name=3, active=True))),
])
def test_ne(a, b):
    assert a != b


class test_FieldDescriptor:

    def test_getattr(self):
        u = User(id=1, username=2, account=Account(id=3, name=4))

        assert User.id.getattr(u) == 1
        assert User.username.getattr(u) == 2
        assert User.account.id.getattr(u) == 3
        assert User.account.name.getattr(u) == 4


@pytest.mark.parametrize('record', [
    Account(id=None, name=None),
    Account(id='123', name='123'),
    Account(id='123', name='123', active=False),
    User(id='123',
         username='foo',
         account=Account(id='123', name='Foo', active=True)),
    User(id='123', username='foo', account=None),
    User(id=None, username=None, account=None),
    AccountList(accounts=[Account(id=None, name=None)]),
    AccountMap(accounts={'foo': Account(id=None, name='foo')}),
])
def test_dumps(record):
    assert record.loads(record.dumps()) == record
    assert repr(record)


def test_subclass_default_values():

    class X(Record):
        x: int
        y: int = None

    class Z(X):
        z: int = None

    assert X(x=None).y is None
    assert X(x=None, y=303).y == 303
    assert Z(x=None).y is None
    assert Z(x=None).z is None
    assert Z(x=None, y=101, z=303).y == 101
    assert Z(x=None, y=101, z=303).z == 303


def test_subclass_preserves_required_values():

    class X(Record):
        x: int

    class Y(X):
        y: int

    assert not Y._options.defaults
    assert not Y._options.optionalset
    with pytest.raises(TypeError):
        Y(y=10)
    Y(10, 20)


class test_too_many_arguments_raises_TypeError():
    class X(Record):
        x: int

    class Y(X):
        y: int

    with pytest.raises(TypeError):
        Y(10, 20, 30)
