import pytest
from case import ANY, Mock
from faust import App, Record, topic
from faust.serializers import codecs
from faust.types.models import ModelT
from faust.utils.compat import want_bytes
from faust.utils.futures import done_future

test_topic = topic('test')


class Key(Record, serializer='json'):
    value: int


class Value(Record, serializer='json'):
    amount: float


@pytest.fixture
def app():
    instance = App('testid')
    instance.producer = Mock(name='producer')
    return instance


def setup_producer(app):
    app.producer.start.return_value = done_future()
    app.producer.send.return_value = done_future()
    app.producer.send_and_wait.return_value = done_future()


@pytest.mark.asyncio
@pytest.mark.parametrize('key,wait,topic,expected_topic,key_serializer', [
    ('key', True, test_topic, test_topic.topics[0], None),
    (Key(value=10), True, test_topic, test_topic.topics[0], None),
    ({'key': 'k'}, True, test_topic, test_topic.topics[0], 'json'),
    (None, True, 'topic', 'topic', None),
    (b'key', False, test_topic, test_topic.topics[0], None),
    ('key', False, 'topic', 'topic', None),
])
async def test_send(key, wait, topic, expected_topic, key_serializer, app):
    event = Value(amount=0.0)
    setup_producer(app)
    await app.send(topic, key, event, key_serializer=key_serializer, wait=wait)
    # do it twice so producer_started is also True
    await app.send(topic, key, event, key_serializer=key_serializer, wait=wait)
    expected_sender = (
        app.producer.send_and_wait
        if wait else app.producer.send
    )
    if key is not None:
        if isinstance(key, ModelT):
            expected_key = key.dumps()
        elif key_serializer:
            expected_key = codecs.dumps(key_serializer, key)
        else:
            expected_key = want_bytes(key)
    else:
        expected_key = None
    expected_sender.assert_called_with(
        expected_topic, expected_key, event.dumps(),
    )


def test_add_task(app, patching):
    ensure_future = patching('asyncio.ensure_future')

    async def foo():
        ...

    app.add_task(foo)
    ensure_future.assert_called()


def test_stream(app):
    s = app.stream(test_topic)
    assert s.topics == [test_topic]
    assert s.app == app


def test_stream_with_coroutine(app):

    async def coro(it):
        ...

    s = app.stream(test_topic, coro)
    assert s.topics == [test_topic]
    assert s._coroutines[test_topic]
    assert s.app == app


@pytest.mark.asyncio
async def test_on_stop_producer(app):
    app._producer.stop.return_value = done_future()
    await app.stop()
    app._producer.stop.assert_called_with()


def test_new_producer(app):
    app._producer = None
    app._transport = Mock(name='transport')
    assert app._new_producer() is app._transport.create_producer.return_value
    app._transport.create_producer.assert_called_with(beacon=ANY)
    assert app.producer is app._transport.create_producer.return_value


def test_create_transport(app, patching):
    by_url = patching('faust.transport.by_url')
    assert app._create_transport() is by_url.return_value.return_value
    assert app.transport is by_url.return_value.return_value
    by_url.assert_called_with(app.url)
    by_url.return_value.assert_called_with(app.url, app, loop=app.loop)
    app.transport = 10
    assert app.transport == 10
