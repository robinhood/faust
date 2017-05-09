import pytest
from faust import Record


class Account(Record):
    id: str
    name: str
    active: bool = True


class User(Record):
    id: str
    username: str
    account: Account


def test_constructor():
    with pytest.raises(TypeError):
        Account(id='123')
    with pytest.raises(TypeError):
        Account(name='foo')
    with pytest.raises(TypeError):
        Account(unknown_argument=303)
    account = Account(id='123', name='foo', req=123)
    assert account.id == '123'
    assert account.name == 'foo'
    assert account.active
    assert not Account(id='123', name='foo', active=False).active
    assert account.req == 123


def test_constructor_from_data():
    with pytest.raises(TypeError):
        Account({'id': '123'})
    with pytest.raises(TypeError):
        Account({'name': 'foo'})
    with pytest.raises(TypeError):
        Account({'unknown_argument': 303})
    account = Account({'id': '123', 'name': 'foo'}, req=123)
    assert account.id == '123'
    assert account.name == 'foo'
    assert account.active
    assert account.req == 123
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
