from datetime import datetime
from typing import Dict, List, Mapping, Optional, Set, Tuple
from faust import Record
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


def test_constructor_from_data():
    with pytest.raises(TypeError):
        Account({'id': '123'})
    with pytest.raises(TypeError):
        Account({'name': 'foo'})
    with pytest.raises(TypeError):
        Account({'unknown_argument': 303})
    account = Account({'id': '123', 'name': 'foo'})
    assert account.id == '123'
    assert account.name == 'foo'
    assert account.active
    assert not Account({'id': '123', 'name': 'foo', 'active': False}).active


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
])
def test_dumps(record):
    assert record.loads(record.dumps()) == record
    assert repr(record)
    assert record.as_schema()
    assert record.as_avro_schema()
