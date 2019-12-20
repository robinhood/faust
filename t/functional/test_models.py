import abc
from datetime import datetime
from decimal import Decimal
from typing import ClassVar, Dict, List, Mapping, Optional, Set, Tuple
from dateutil.parser import parse as parse_date
import faust
from faust.exceptions import SecurityError, ValidationError
from faust.models import maybe_model
from faust.models.fields import (
    BytesField,
    DatetimeField,
    DecimalField,
    FieldDescriptor,
    FloatField,
    IntegerField,
    StringField,
)
from mode.utils.logging import get_logger
from faust.models.tags import (
    Secret,
    Sensitive,
    _FrameLocal,
    allow_protected_vars,
)
from faust.types import ModelT
from faust.utils import json
import pytest

logger = get_logger(__name__)


class Record(faust.Record, serializer='json'):
    # make sure all models in this test uses json as the serializer.
    ...


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

        def __post_init__(self):
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


def test_datetimes():

    class Date(Record, coerce=True):
        date: datetime

    class OptionalDate(Record, coerce=True):
        date: Optional[datetime]

    class TupleOfDate(Record, coerce=True):
        dates: Tuple[datetime, ...]

    class SetOfDate(Record, coerce=True):
        dates: Set[datetime]

    class MapOfDate(Record, coerce=True):
        dates: Mapping[int, datetime]

    class ListOfDate(Record, coerce=True):
        dates: List[datetime]

    class OptionalListOfDate(Record, coerce=True):
        dates: List[datetime] = None

    class OptionalListOfDate2(Record, coerce=True):
        dates: Optional[List[datetime]]

    assert isinstance(OptionalDate.date, DatetimeField)

    n1 = datetime.utcnow()
    assert Date.loads(Date(date=n1).dumps()).date == n1
    assert OptionalDate.loads(OptionalDate(date=n1).dumps()).date == n1
    assert OptionalDate.loads(OptionalDate(date=None).dumps()).date is None
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
        dates={'A': n1, 'B': n2}).dumps()).dates == {'A': n1, 'B': n2}

    datelist = ListOfDate.from_data({
        'dates': [n1.isoformat(), n2.isoformat()]})
    assert isinstance(datelist.dates[0], datetime)
    assert isinstance(datelist.dates[1], datetime)


def test_datetimes__isodates_compat():

    class Date(Record, coerce=False, isodates=True):
        date: datetime

    class TupleOfDate(Record, coerce=False, isodates=True):
        dates: Tuple[datetime, ...]

    class SetOfDate(Record, coerce=False, isodates=True):
        dates: Set[datetime]

    class MapOfDate(Record, coerce=False, isodates=True):
        dates: Mapping[int, datetime]

    class ListOfDate(Record, coerce=False, isodates=True):
        dates: List[datetime]

    class OptionalListOfDate(Record, coerce=False, isodates=True):
        dates: List[datetime] = None

    class OptionalListOfDate2(Record, coerce=False, isodates=True):
        dates: Optional[List[datetime]]

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
        dates={'A': n1, 'B': n2}).dumps()).dates == {'A': n1, 'B': n2}

    datelist = ListOfDate.from_data({
        'dates': [n1.isoformat(), n2.isoformat()]})
    assert isinstance(datelist.dates[0], datetime)
    assert isinstance(datelist.dates[1], datetime)


def test_decimals():

    class IsDecimal(Record, coerce=True, serializer='json'):
        number: Decimal

    class ListOfDecimal(Record, coerce=True, serializer='json'):
        numbers: List[Decimal]

    class OptionalListOfDecimal(Record, coerce=True, serializer='json'):
        numbers: List[Decimal] = None

    class OptionalListOfDecimal2(Record, coerce=True, serializer='json'):
        numbers: Optional[List[Decimal]]

    class TupleOfDecimal(Record, coerce=True, serializer='json'):
        numbers: Tuple[Decimal, ...]

    class SetOfDecimal(Record, coerce=True, serializer='json'):
        numbers: Set[Decimal]

    class MapOfDecimal(Record, coerce=True, serializer='json'):
        numbers: Mapping[str, Decimal]

    n1 = Decimal('1.31341324')
    assert IsDecimal.loads(IsDecimal(number=n1).dumps()).number == n1
    n2 = Decimal('3.41569')
    assert ListOfDecimal.loads(ListOfDecimal(
        numbers=[n1, n2]).dumps()).numbers == [n1, n2]
    assert OptionalListOfDecimal.loads(OptionalListOfDecimal(
        numbers=None).dumps()).numbers is None
    assert OptionalListOfDecimal.loads(OptionalListOfDecimal(
        numbers=[n2, n1]).dumps()).numbers == [n2, n1]
    assert OptionalListOfDecimal2.loads(OptionalListOfDecimal2(
        numbers=None).dumps()).numbers is None
    assert OptionalListOfDecimal2.loads(OptionalListOfDecimal2(
        numbers=[n1, n2]).dumps()).numbers == [n1, n2]
    assert TupleOfDecimal.loads(TupleOfDecimal(
        numbers=(n1, n2)).dumps()).numbers == (n1, n2)
    assert TupleOfDecimal.loads(TupleOfDecimal(
        numbers=(n2,)).dumps()).numbers == (n2,)
    assert SetOfDecimal.loads(SetOfDecimal(
        numbers={n1, n2}).dumps()).numbers == {n1, n2}
    assert MapOfDecimal.loads(MapOfDecimal(
        numbers={'A': n1, 'B': n2}).dumps()).numbers == {'A': n1, 'B': n2}

    dlist = ListOfDecimal.from_data({'numbers': ['1.312341', '3.41569']})
    assert isinstance(dlist.numbers[0], Decimal)
    assert isinstance(dlist.numbers[1], Decimal)


def test_decimals_compat():

    class IsDecimal(Record, coerce=False, decimals=True, serializer='json'):
        number: Decimal

    class ListOfDecimal(Record,
                        coerce=False,
                        decimals=True,
                        serializer='json'):
        numbers: List[Decimal]

    class OptionalListOfDecimal(Record,
                                coerce=False,
                                decimals=True,
                                serializer='json'):
        numbers: List[Decimal] = None

    class OptionalListOfDecimal2(Record,
                                 coerce=False,
                                 decimals=True,
                                 serializer='json'):
        numbers: Optional[List[Decimal]]

    class TupleOfDecimal(Record,
                         coerce=False,
                         decimals=True,
                         serializer='json'):
        numbers: Tuple[Decimal, ...]

    class SetOfDecimal(Record,
                       coerce=False,
                       decimals=True,
                       serializer='json'):
        numbers: Set[Decimal]

    class MapOfDecimal(Record,
                       coerce=False,
                       decimals=True,
                       serializer='json'):
        numbers: Mapping[str, Decimal]

    n1 = Decimal('1.31341324')
    assert IsDecimal.loads(IsDecimal(number=n1).dumps()).number == n1
    n2 = Decimal('3.41569')
    assert ListOfDecimal.loads(ListOfDecimal(
        numbers=[n1, n2]).dumps()).numbers == [n1, n2]
    assert OptionalListOfDecimal.loads(OptionalListOfDecimal(
        numbers=None).dumps()).numbers is None
    assert OptionalListOfDecimal.loads(OptionalListOfDecimal(
        numbers=[n2, n1]).dumps()).numbers == [n2, n1]
    assert OptionalListOfDecimal2.loads(OptionalListOfDecimal2(
        numbers=None).dumps()).numbers is None
    assert OptionalListOfDecimal2.loads(OptionalListOfDecimal2(
        numbers=[n1, n2]).dumps()).numbers == [n1, n2]
    assert TupleOfDecimal.loads(TupleOfDecimal(
        numbers=(n1, n2)).dumps()).numbers == (n1, n2)
    assert TupleOfDecimal.loads(TupleOfDecimal(
        numbers=(n2,)).dumps()).numbers == (n2,)
    assert SetOfDecimal.loads(SetOfDecimal(
        numbers={n1, n2}).dumps()).numbers == {n1, n2}
    assert MapOfDecimal.loads(MapOfDecimal(
        numbers={'A': n1, 'B': n2}).dumps()).numbers == {'A': n1, 'B': n2}

    dlist = ListOfDecimal.from_data({'numbers': ['1.312341', '3.41569']})
    assert isinstance(dlist.numbers[0], Decimal)
    assert isinstance(dlist.numbers[1], Decimal)


def test_custom_coercion():

    class Foo:

        def __init__(self, value: int):
            assert isinstance(value, int)
            self.value: int = value

        def __eq__(self, other):
            if other.__class__ is self.__class__:
                return other.value == self.value
            return NotImplemented

        def __hash__(self):
            return hash(self.value)

        def __json__(self):
            return self.value

        def __repr__(self):
            return f'<{type(self).__name__}: {self.value}>'

    class CanFooModel(Record,
                      abstract=True,
                      coercions={Foo: Foo},
                      serializer='json'):
        ...

    class IsFoo(CanFooModel, serializer='json'):
        foo: Foo

    class ListOfFoo(CanFooModel, serializer='json'):
        foos: List[Foo]

    class OptionalListOfFoo(CanFooModel, serializer='json'):
        foos: List[Foo] = None

    class OptionalListOfFoo2(CanFooModel):
        foos: Optional[List[Foo]]

    class TupleOfFoo(CanFooModel):
        foos: Tuple[Foo, ...]

    class SetOfFoo(CanFooModel):
        foos: Set[Foo]

    class MapOfFoo(CanFooModel):
        foos: Mapping[str, Foo]

    n1 = Foo(101)
    assert IsFoo.loads(IsFoo(foo=n1).dumps()).foo == n1
    n2 = Foo(202)
    assert ListOfFoo.loads(ListOfFoo(
        foos=[n1, n2]).dumps()).foos == [n1, n2]
    assert OptionalListOfFoo.loads(OptionalListOfFoo(
        foos=None).dumps()).foos is None
    assert OptionalListOfFoo.loads(OptionalListOfFoo(
        foos=[n2, n1]).dumps()).foos == [n2, n1]
    assert OptionalListOfFoo2.loads(OptionalListOfFoo2(
        foos=None).dumps()).foos is None
    assert OptionalListOfFoo2.loads(OptionalListOfFoo2(
        foos=[n1, n2]).dumps()).foos == [n1, n2]
    assert TupleOfFoo.loads(TupleOfFoo(
        foos=(n1, n2)).dumps()).foos == (n1, n2)
    assert TupleOfFoo.loads(TupleOfFoo(
        foos=(n2,)).dumps()).foos == (n2,)
    assert SetOfFoo.loads(SetOfFoo(
        foos={n1, n2}).dumps()).foos == {n1, n2}
    assert MapOfFoo.loads(MapOfFoo(
        foos={'A': n1, 'B': n2}).dumps()).foos == {'A': n1, 'B': n2}


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

    expected_set = {a1, a2, a3}
    acc1 = AccountSet.loads(AccountSet(accounts={a1, a2, a3}).dumps()).accounts
    assert isinstance(acc1, set)
    assert len(acc1) == len(expected_set)
    for acc in acc1:
        assert acc in expected_set
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
    assert sorted(AccountSet.loads(FREFAccountSet(
        accounts={a1, a2, a3}).dumps()).accounts) == sorted({a1, a2, a3})
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
     User(id=1, username=2, account=Account(id=1, name=2))),
    (User(id=1, username=2, account=Account(id=1, name=2, active=False)),
     User(id=1, username=2, account=Account(id=1, name=2, active=False))),
])
def test_eq(a, b):
    assert a == b


def test_eq__incompatible():
    assert Account(id=1, name=2) != object()


@pytest.mark.parametrize('a,b', [
    (User(id=1, username=2, account=Account(id=1, name=2)),
     User(id=2, username=2, account=Account(id=2, name=2))),
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


def test_json():
    account = Account(id=1, name=2)
    user = User(1, 2, account)

    payload = json.dumps(user)
    deser = json.loads(payload)
    assert deser == {
        'id': 1,
        'username': 2,
        'account': {
            'id': 1,
            'name': 2,
            'active': True,
            '__faust': {'ns': Account._options.namespace},
        },
        '__faust': {'ns': User._options.namespace},
    }

    assert user.__json__() == {
        'id': 1,
        'username': 2,
        'account': account,
        '__faust': {'ns': User._options.namespace},
    }

    assert User.from_data(deser) == user


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
        z: int = None

    class Y(X):
        y: int

    assert Y._options.defaults == {'z': None}
    assert Y._options.optionalset == {'z'}
    with pytest.raises(TypeError):
        Y(y=10)
    with pytest.raises(TypeError):
        Y(y=10, z=10)
    Y(x=10, y=20)


def test_too_many_arguments_raises_TypeError():
    class X(Record):
        x: int

    class Y(X):
        y: int

    with pytest.raises(TypeError) as einfo:
        Y(10, 20, 30)
    reason = str(einfo.value)
    assert '__init__() takes' in reason


def test_fields_with_concrete_polymorphic_type__dict():

    class X(Record, isodates=True):
        foo: int
        details: dict


def test_fields_with_concrete_polymorphic_type__dict_optional():

    class X(Record, isodates=True):
        username: str
        metric_name: str
        value: float
        timestamp: datetime
        details: dict = None


def test_fields_with_concrete_polymorphic_type__tuple():

    class X(Record, isodates=True):
        foo: int
        details: tuple


def test_fields_with_concrete_polymorphic_type__list():

    class X(Record, isodates=True):
        foo: int
        details: list


def test_fields_with_concrete_polymorphic_type__set():

    class X(Record, isodates=True):
        foo: int
        details: set


def test_fields_with_concrete_polymorphic_type__frozenset():

    class X(Record, isodates=True):
        foo: int
        details: frozenset


def test_supports_post_init():

    class X(Record):
        x: int
        y: int

        def __post_init__(self):
            self.z: int = self.x + self.y

    x = X(1, 3)
    assert x.z == 4


def test_default_no_blessed_key():

    class X(Record):
        a: int

    class LooksLikeX(Record):
        a: int

    class Y(Record):
        x: X

    x = LooksLikeX(303)
    y = Y(x)

    data = Y.dumps(y, serializer='json')
    y2 = Y.loads(data, serializer='json')
    assert isinstance(y2.x, X)


def test_default_multiple_levels_no_blessed_key():

    class StdAttribution(Record):
        first_name: str
        last_name: str

    class Address(Record):
        country: str

    class Account(StdAttribution):
        address: Address

    class Event(Record):
        account: Account

    event = Event(account=Account(
        first_name='George',
        last_name='Costanza',
        address=Address('US'),
    ))
    s = event.loads(event.dumps(serializer='json'), serializer='json')
    assert isinstance(s.account, Account)
    assert isinstance(s.account.address, Address)


def test_polymorphic_fields(app):

    class X(Record):
        a: int

    class LooksLikeX(Record, polymorphic_fields=True):
        a: int

    class Y(Record):
        x: LooksLikeX

    x = LooksLikeX(303)
    y = Y(x)

    data = Y.dumps(y, serializer='json')
    y2 = app.serializers.loads_key(Y, data, serializer='json')
    assert isinstance(y2.x, LooksLikeX)


def test_compat_enabled_blessed_key(app):

    class X(Record):
        a: int

    class LooksLikeX(Record, allow_blessed_key=True):
        a: int

    class Y(Record):
        x: LooksLikeX

    x = LooksLikeX(303)
    y = Y(x)

    data = Y.dumps(y, serializer='json')
    y2 = app.serializers.loads_key(Y, data, serializer='json')
    assert isinstance(y2.x, LooksLikeX)


def test__polymorphic_fields_deeply_nested():

    class BaseAttribution(Record, abc.ABC):

        def __post_init__(self, *args, **kwargs) -> None:
            self.data_store = None

    class AdjustData(Record):
        activity_kind: str

    class Event(Record):
        category: str
        event: str
        data: AdjustData

    class AdjustRecord(BaseAttribution):
        event: Event

    x = AdjustRecord(Event(
        category='foo',
        event='bar',
        data=AdjustData('baz'),
    ))
    value = x.dumps(serializer='json')
    value_dict = json.loads(value)
    value_dict['event']['__faust']['ns'] = 'x.y.z'
    model = AdjustRecord.from_data(value_dict)
    assert isinstance(model.event, Event)
    assert isinstance(model.event.data, AdjustData)


def test_compat_blessed_key_deeply_nested():

    class BaseAttribution(Record, abc.ABC):

        def __post_init__(self, *args, **kwargs) -> None:
            self.data_store = None

    class AdjustData(Record):
        activity_kind: str

    class Event(Record):
        category: str
        event: str
        data: AdjustData

    class AdjustRecord(BaseAttribution):
        event: Event

    x = AdjustRecord(Event(
        category='foo',
        event='bar',
        data=AdjustData('baz'),
    ))
    value = x.dumps(serializer='json')
    value_dict = json.loads(value)
    value_dict['event']['__faust']['ns'] = 'x.y.z'
    model = AdjustRecord.from_data(value_dict)
    assert isinstance(model.event, Event)
    assert isinstance(model.event.data, AdjustData)


ADTRIBUTE_PAYLOAD = '''
{"user": {"username": "3da6ef8f-aed1-47e7-ad4f-034363d0565b",
 "secret": null, "__faust": {"ns": "trebuchet.models.logging.User"}},
 "device": {"platform": "iOS",
 "device_id": "F5FA74CE-0F17-491F-A6B1-AAE1D036CBF2",
 "os_version": "11.2.6", "manufacturer": "phone",
 "device_version": "iPhone8,2", "screen_resolution": null,
 "source": null, "campaign": null,
 "campaign_version": null, "adid": null,
 "engagement_time": null,
 "__faust": {"ns": "trebuchet.models.logging.Device"}},
 "app": {"version": "4667", "app_id": "com.robinhood.release.Robinhood",
 "build_num": null, "locale": null, "language": null,
 "__faust": {"ns": "trebuchet.models.logging.App"}},
 "event": {"category": "adjust_data", "event": "attribution",
 "experiments": null, "session_id": null, "data":
 {"activity_kind": "session", "network_name": "Organic",
 "adid": "04df21c2ef05a91598e13c82096d921b",
 "tracker": "494pkq", "reftag": "oJuX55u4N4OI4",
 "nonce": "mt5lyv18d", "campaign_name": "",
 "adgroup_name": "", "creative_name": "", "click_referer": "",
 "is_organic": "1", "reattribution_attribution_window": "",
 "impression_attribution_window": "", "store": "itunes",
 "match_type": "", "platform_adid": "", "search_term": "",
 "event_name": "", "installed_at": "2017-09-02 00:28:03.000",
 "engagement_time": null, "deeplink": "",
 "source_user": "", "__faust": {
 "ns": "trebuchet.models.logging_data.AdjustData"}},
 "__faust": {"ns": "trebuchet.models.logging.Event"}},
 "timestamp": "2018-03-22 16:57:19.000", "client_ip": "174.207.10.101",
 "event_hash": "50c9a0e19b9644abe269aadcea9e7526", "__faust": {
    "ns": "trebuchet.models.logging.LoggingEvent"}}
'''


def test_adtribute_payload(app):

    class BaseAttribution(Record, abc.ABC):

        def __post_init__(self) -> None:
            self.data_store = None

    class AdjustData(Record):

        activity_kind: str
        network_name: str
        adid: str
        tracker: str
        reftag: str
        nonce: str
        campaign_name: str = None
        adgroup_name: str = None
        creative_name: str = None
        click_referer: str = None
        is_organic: str = None
        reattribution_attribution_window: str = None
        impression_attribution_window: str = None
        store: str = None
        match_type: str = None
        platform_adid: str = None
        search_term: str = None
        event_name: str = None
        installed_at: str = None
        engagement_time: str = None
        deeplink: str = None
        source_user: str = None

    class User(Record):
        username: str

    class App(Record):
        version: str
        app_id: str

    class Device(Record):
        platform: str
        device_id: str
        os_version: str
        device_version: str
        manufacturer: str

    class Event(Record):
        category: str
        event: str
        data: AdjustData

    class AdjustRecord(BaseAttribution):
        user: User
        device: Device
        app: App
        event: Event
        timestamp: str
        client_ip: str = None
        event_hash: str = None

    def __post_init__(self) -> None:
        self.data_store = None

    app.serializers.loads_value(
        AdjustRecord, ADTRIBUTE_PAYLOAD, serializer='json')


def test_overwrite_asdict():

    with pytest.raises(RuntimeError):

        class R(Record):

            def asdict(self):
                return {'foo': 1}


def test_prepare_dict():

    class Quote(Record):
        ask_price: float = None
        bid_price: float = None

        def _prepare_dict(self, payload):
            return {k: v for k, v in payload.items() if v is not None}

    assert Quote().asdict() == {}
    assert Quote(1.0, 2.0).asdict() == {'ask_price': 1.0, 'bid_price': 2.0}
    assert Quote(None, 2.0).asdict() == {'bid_price': 2.0}


def test_custom_init_calling_model_init():

    class Quote(Record):
        ask_price: float
        bid_price: float

        def __init__(self, ask_price: str, bid_price: str, **kwargs):
            self._model_init(ask_price, bid_price, **kwargs)

    q1 = Quote(1.0, 2.0)
    assert q1.ask_price == 1.0
    assert q1.bid_price == 2.0

    with pytest.raises(TypeError):
        Quote(1.0, 2.0, foo=1)


def test_repr():
    assert repr(Account.id)


def test_ident():
    assert Account.id.ident == 'Account.id'


def test_list_field_refers_to_self():

    class X(Record):
        id: int
        xs: List['X']

    x = X(1, [X(2, [X(3, [])])])

    as_json = x.dumps(serializer='json')
    loads = X.loads(as_json, serializer='json')
    assert loads == x

    assert isinstance(loads.xs[0], X)
    assert isinstance(loads.xs[0].xs[0], X)


def test_optional_modelfield():

    class X(Record):
        id: int

    class Y(Record):
        x: Optional[X] = None

    y = Y(X(30))

    as_json = y.dumps(serializer='json')
    loads = Y.loads(as_json, serializer='json')
    assert loads == y

    assert isinstance(loads.x, X)


def test_optional_modelfield_with_coercion():
    class X(Record, coercions={str: str}):
        y: Optional[str]

    x = X(y='test')

    assert x.y == 'test'


@pytest.mark.parametrize('flag,expected_default', [
    ('isodates', False),
    ('include_metadata', True),
    ('polymorphic_fields', False),
])
def test_subclass_inherit_flags(flag, expected_default):

    class BaseX(Record):
        x: datetime

    X = type('X', (BaseX,), {}, **{flag: not expected_default})
    Y = type('Y', (X,), {})
    Z = type('Z', (Y,), {}, **{flag: expected_default})

    assert getattr(BaseX._options, flag) is expected_default
    assert getattr(X._options, flag) is not expected_default
    assert getattr(Y._options, flag) is not expected_default
    assert getattr(Z._options, flag) is expected_default


def test_abstract_model_repr():

    class MyBase(faust.Record, abstract=True):
        ...

    assert MyBase.__is_abstract__
    with pytest.raises(NotImplementedError):
        MyBase()


def test_raises_when_defaults_in_wrong_order():

    with pytest.raises(TypeError):
        class X(Record):
            foo: str
            bar: int = 3
            baz: str


def test_maybe_namespace_raises_for_missing_abstract_type():
    class X(Record):
        foo: str

    with pytest.raises(KeyError):
        X._maybe_namespace({X._blessed_key: {'ns': 'a.b.c.d.e.f'}},
                           preferred_type=ModelT)


def test_compat_loads_DeprecationWarning():
    class X(Record):
        foo: str

    payload = X('foo').dumps(serializer='json')
    with pytest.warns(DeprecationWarning):
        X.loads(payload, default_serializer='json')


def test_model_overriding_Options_sets_options():

    class X(Record):
        foo: str

        class Options:
            coercions = {'foo': 'bar'}
            foo = 1.345

    with pytest.raises(AttributeError):
        X.Options
    assert X._options.coercions == {'foo': 'bar'}
    assert X._options.foo == 1.345


def test_model_with_custom_hash():
    class X(Record):
        foo: int

        def __hash__(self):
            return self.foo

    assert hash(X(123)) == 123


def test_model_with_custom_eq():
    class X(Record):
        foo: bool

        def __eq__(self, other):
            return True

    assert X(10) == X(20)


def test_Record_comparison():
    class X(Record):
        x: int
        y: int

    assert X(10, 30) > X(8, 24)
    assert X(10, 30) >= X(10, 30)
    assert X(10, 30) < X(10, 40)
    assert X(10, 40) <= X(10, 40)

    with pytest.raises(TypeError):
        X(10) >= object()
    with pytest.raises(TypeError):
        X(10) <= object()
    with pytest.raises(TypeError):
        X(10) < object()
    with pytest.raises(TypeError):
        X(10) > object()
    with pytest.raises(TypeError):
        object() > X(10)
    with pytest.raises(TypeError):
        object() >= X(10)
    with pytest.raises(TypeError):
        object() < X(10)
    with pytest.raises(TypeError):
        object() <= X(10)


def test_maybe_model():

    class X(Record):
        x: int
        y: int

    assert maybe_model('foo') == 'foo'
    assert maybe_model(1) == 1
    assert maybe_model(1.01) == 1.01

    x1 = X(10, 20)
    assert maybe_model(json.loads(x1.dumps(serializer='json'))) == x1


def test_StringField():

    class Moo(Record):
        foo: str = StringField(max_length=10, min_length=3, allow_blank=False)

    too_long_moo = Moo('thequickbrownfoxjumpsoverthelazydog')
    too_short_moo = Moo('xo')
    perfect_moo = Moo('foobar')

    assert perfect_moo.is_valid()
    assert not too_long_moo.is_valid()
    assert not too_short_moo.is_valid()

    assert not perfect_moo.validation_errors
    assert too_long_moo.validation_errors
    assert too_short_moo.validation_errors

    assert too_long_moo.foo == 'thequickbrownfoxjumpsoverthelazydog'
    assert too_short_moo.foo == 'xo'
    assert perfect_moo.foo == 'foobar'

    assert not Moo('').is_valid()
    assert Moo('').validation_errors
    assert 'blank' in str(Moo('').validation_errors[0])


def test_StringField_optional__explicit():

    class Moo(Record):
        foo: str = StringField(max_length=10,
                               min_length=3,
                               required=False,
                               allow_blank=False)

    moo = Moo()
    assert moo.foo is None
    assert moo.is_valid()


def test_StringField_optional__Optional():

    class Moo(Record):
        foo: Optional[str] = StringField(max_length=10,
                                         min_length=3,
                                         allow_blank=False)

    moo = Moo()
    assert moo.foo is None
    assert moo.is_valid()


def test_validation_ensures_types_match():

    class Order(Record, validation=True):
        price: Decimal = DecimalField()
        quantity: int = IntegerField()
        side: str = StringField()
        foo: float = FloatField(required=False, default=3.33)

    # Field descriptors are not considered defaults
    with pytest.raises(TypeError):
        Order()
    with pytest.raises(TypeError):
        Order(price=Decimal('3.13'))
    with pytest.raises(TypeError):
        Order(price=Decimal('3.13'), quantity=30)

    with pytest.raises(ValidationError):
        Order(price='NaN', quantity=2, side='BUY')
    with pytest.raises(ValueError):
        Order(price='3.13', quantity='xyz', side='BUY')

    assert Order(price=Decimal(3.13), quantity=10, side='BUY')

    order = Order(price='3.13', quantity='10', side='BUY', foo='3.33')
    assert order.price == Decimal('3.13')
    assert isinstance(order.price, Decimal)
    assert order.quantity == 10
    assert order.side == 'BUY'
    assert order.foo == 3.33
    assert isinstance(order.foo, float)


def test_Decimal_max_digits():

    class X(Record, validation=True):
        foo: Decimal = DecimalField(max_digits=3)

    with pytest.raises(ValidationError):
        X(foo='3333.3333333333')

    assert X(foo='33.33333333333333333').foo


def test_StringField_trim_whitespace():

    class X(Record, validation=True):
        foo: str = StringField(trim_whitespace=True)

    assert X(foo='  the quick    ').foo == 'the quick'


def test_BytesField():

    class X(Record, validation=True):
        foo: bytes = BytesField(max_length=10, min_length=3)

    assert X(foo='foo').foo == b'foo'


def test_BytesField_trim_whitespace():

    class X(Record, validation=True):
        foo: bytes = BytesField(trim_whitespace=True)

    assert X(foo='  the quick    ').foo == b'the quick'


def test_field_descriptors_may_mix_with_non_defaults():

    class Person(faust.Record, validation=True):
        age: int = IntegerField(min_value=18, max_value=200)
        name: str

    with pytest.raises(ValidationError):
        Person(age=9, name='Robin')

    with pytest.raises(ValidationError):
        Person(age=203, name='Abraham Lincoln')


def test_field_descriptors_throws_type_error():

    class Person(faust.Record):
        age: int
        name: str

    person = Person(age='Batman', name='Robin')
    assert person.validation_errors


def test_implicit_descritor_types():

    class X(Record):
        a: int
        b: float
        c: str
        d: bytes
        e: Decimal

    assert isinstance(X.a, IntegerField)
    assert isinstance(X.b, FloatField)
    assert isinstance(X.c, StringField)
    assert isinstance(X.d, BytesField)
    assert isinstance(X.e, DecimalField)


def test_exclude():

    class X(Record):
        a: str
        b: str
        c: str = StringField(exclude=True)

    x = X('A', 'B', 'C')

    assert X.a.required
    assert X.b.required
    assert X.c.required

    assert not X.a.exclude
    assert not X.b.exclude
    assert X.c.exclude

    assert x.asdict() == {'a': 'A', 'b': 'B'}

    assert 'c' not in x.to_representation()


def test_custom_field_validation():

    class ChoiceField(FieldDescriptor[str]):

        def __init__(self, choices: List[str], **kwargs) -> None:
            self.choices = choices
            # Must pass any custom args to init,
            # so we pass the choices keyword argument also here.
            super().__init__(choices=choices, **kwargs)

        def validate(self, value: str):
            if value not in self.choices:
                choices = ', '.join(self.choices)
                yield self.validation_error(
                    f'Field {self.field} must be one of {choices}')

    class Order(faust.Record, validation=True):
        side: str = ChoiceField(['SELL', 'BUY'])

    with pytest.raises(ValidationError):
        Order(side='LEFT')

    class Order2(faust.Record, validation=True):
        side: str = ChoiceField(['SELL', 'BUY'], required=False)

    assert Order2()


def test_custom_field__internal_errot():

    class XField(FieldDescriptor[str]):

        def prepare_value(self, value, coerce=None):
            if coerce:
                raise RuntimeError()
            return value

    class Foo(Record, coerce=False):
        foo: str = XField()

    f = Foo('foo')
    assert f.validation_errors
    assert 'RuntimeError' in str(f.validation_errors[0])


def test_datetime_does_not_coerce():

    class X(Record, coerce=False):
        d: datetime

    date_string = 'Sat Jan 12 00:44:36 +0000 2019'
    assert X(date_string).d == date_string


def test_datetime_custom_date_parser():

    class X(Record, coerce=True, date_parser=parse_date):
        d: datetime

    date_string = 'Sat Jan 12 00:44:36 +0000 2019'
    assert X.from_data({'d': date_string}).d == parse_date(date_string)


def test_float_does_not_coerce():

    class X(Record, coerce=False):
        f: float
    X.make_final()  # <-- just to test it still works for non-lazy creation

    assert X('3.14').f == '3.14'


def test_payload_with_reserved_keyword():

    class X(Record):
        location: str = StringField(input_name='in')
        foo: str = StringField(required=False, default='BAR',
                               input_name='bar', output_name='foobar')

    with pytest.raises(TypeError):
        X()

    assert X('foo').location == 'foo'
    assert X(location='FOO').location == 'FOO'

    d = X.from_data({'in': 'foo', 'bar': 'bar'})
    assert d.location == 'foo'
    assert d.foo == 'bar'

    assert d.asdict() == {
        'in': 'foo',
        'foobar': 'bar',
    }


class LazyX(Record, lazy_creation=True):
    y: 'EagerY'


class EagerY(Record):
    x: LazyX


def test_lazy_creation():
    assert LazyX._pending_finalizers
    LazyX.make_final()
    assert LazyX._pending_finalizers is None


def test_Sensitive(*, capsys):

    class Foo(Record):
        name: str
        phone_number: Sensitive[str]

    class Bar(Record):
        alias: str
        foo: Foo

    class Baz(Record):
        alias: str
        bar: List[Bar]

    x = Foo(name='Foo', phone_number='631-342-3412')

    assert Foo._options.has_sensitive_fields
    assert Foo._options.has_tagged_fields
    assert not Foo._options.has_secret_fields
    assert not Foo._options.has_personal_fields

    assert 'phone_number' in Foo._options.sensitive_fields
    assert 'foo' not in Foo._options.sensitive_fields

    # Model with related model that has sensitive fields, is also sensitive.
    assert Bar._options.has_sensitive_fields
    assert 'foo' in Bar._options.sensitive_fields
    assert 'alias' not in Bar._options.sensitive_fields

    assert Baz._options.has_sensitive_fields
    assert 'bar' in Baz._options.sensitive_fields
    assert 'alias' not in Baz._options.sensitive_fields

    assert x.name == 'Foo'
    with pytest.raises(SecurityError):
        str(x.phone_number)
    assert x.phone_number.get_value() == '631-342-3412'

    with pytest.raises(SecurityError):
        f'Name={x.name} Phone={x.phone_number}'

    logger.critical('User foo error %s', x.phone_number)
    stderr_content = capsys.readouterr()
    assert 'Logging error' in stderr_content.err
    assert 'SecurityError' in stderr_content.err

    def exclaim(x: str) -> str:
        assert isinstance(x, _FrameLocal)
        return f'{x}!'

    with pytest.raises(SecurityError):
        exclaim(x.phone_number.get_value())

    def upper(x: str) -> str:
        return x.upper()

    with pytest.raises(SecurityError):
        upper(x.phone_number.get_value())

    with allow_protected_vars():
        assert upper(x.phone_number.get_value()) == '631-342-3412'


def test_Secret(*, caplog):

    class Foo(Record):
        name: str
        phone_number: Secret[str]

    class Bar(Record):
        alias: str
        foo: Foo

    class Baz(Record):
        alias: str
        bar: List[Bar]

    x = Foo(name='Foo', phone_number='631-342-3412')

    assert Foo._options.has_secret_fields
    assert not Foo._options.has_sensitive_fields
    assert Foo._options.has_tagged_fields
    assert not Foo._options.has_personal_fields

    assert 'phone_number' in Foo._options.secret_fields
    assert 'foo' not in Foo._options.secret_fields

    # Model with related model that has sensitive fields, is also sensitive.
    assert Bar._options.has_secret_fields
    assert 'foo' in Bar._options.secret_fields
    assert 'alias' not in Bar._options.secret_fields

    assert Baz._options.has_secret_fields
    assert 'bar' in Baz._options.secret_fields
    assert 'alias' not in Baz._options.secret_fields

    assert x.name == 'Foo'
    assert str(x.phone_number) == x.phone_number.mask
    assert x.phone_number.get_value() == '631-342-3412'

    assert (f'Name={x.name} Phone={x.phone_number}' ==
            f'Name={x.name} Phone={x.phone_number.mask}')

    logger.critical('User foo error %s', x.phone_number)

    assert x.phone_number.get_value() not in caplog.text
    assert x.phone_number.mask in caplog.text
