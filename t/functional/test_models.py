import abc
from datetime import datetime
from decimal import Decimal
from typing import ClassVar, Dict, List, Mapping, Optional, Set, Tuple
import faust
from faust.utils import iso8601
from faust.utils import json
import pytest


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


def test_isodates():

    class Date(Record, isodates=True):
        date: datetime

    class TupleOfDate(Record, isodates=True):
        dates: Tuple[datetime]

    class SetOfDate(Record, isodates=True):
        dates: Set[datetime]

    class MapOfDate(Record, isodates=True):
        dates: Mapping[int, datetime]

    class ListOfDate(Record, isodates=True):
        dates: List[datetime]

    class OptionalListOfDate(Record, isodates=True):
        dates: List[datetime] = None

    class OptionalListOfDate2(Record, isodates=True):
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


def test_decimals():

    class IsDecimal(Record, decimals=True, serializer='json'):
        number: Decimal

    class ListOfDecimal(Record, decimals=True, serializer='json'):
        numbers: List[Decimal]

    class OptionalListOfDecimal(Record, decimals=True, serializer='json'):
        numbers: List[Decimal] = None

    class OptionalListOfDecimal2(Record, decimals=True, serializer='json'):
        numbers: Optional[List[Decimal]]

    class TupleOfDecimal(Record, decimals=True, serializer='json'):
        numbers: Tuple[Decimal]

    class SetOfDecimal(Record, decimals=True, serializer='json'):
        numbers: Set[Decimal]

    class MapOfDecimal(Record, decimals=True, serializer='json'):
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
        foos: Tuple[Foo]

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
    assert User(1, 2, Account(id=1, name=2)).__json__() == {
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


class test_too_many_arguments_raises_TypeError():
    class X(Record):
        x: int

    class Y(X):
        y: int

    with pytest.raises(TypeError) as einfo:
        Y(10, 20, 30)
    reason = str(einfo.value)
    assert reason == '__init__() takes 3 positional arguments but 4 were given'


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


def test_enabled_blessed_key(app):

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


def test_blessed_key_deeply_nested():

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


ADTRIBUTE_PAYLOAD = """
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
"""


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


DATETIME1 = datetime(2012, 6, 5, 13, 33, 0)


@pytest.mark.parametrize('input,expected', [
    (None, None),
    (DATETIME1, DATETIME1),
    (DATETIME1.isoformat(), DATETIME1),

])
def test_parse_iso8601(input, expected):
    assert Record._init_maybe_coerce(
        iso8601.parse, datetime, input) == expected


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


@pytest.mark.parametrize('flag,expected_default', [
    ('isodates', False),
    ('include_metadata', True),
    ('allow_blessed_key', False),
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
