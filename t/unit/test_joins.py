import pytest
from faust import Record
from faust.joins import InnerJoin, Join, LeftJoin, OuterJoin, RightJoin
from mode.utils.mocks import Mock


class User(Record):
    id: str
    name: str


@pytest.mark.asyncio
@pytest.mark.parametrize('join_cls,fields', [
    (Join, (User.id, User.name)),
    (InnerJoin, (User.id, User.name)),
    (LeftJoin, (User.id, User.name)),
    (OuterJoin, (User.id, User.name)),
    (RightJoin, (User.id, User.name)),
])
async def test_Join(join_cls, fields):
    stream = Mock(name='stream')
    j = join_cls(stream=stream, fields=fields)
    assert j.fields
    assert j.stream is stream

    with pytest.raises(NotImplementedError):
        await j.process(Mock(name='event'))
