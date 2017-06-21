import faust
import pytest
from case import ANY, Mock
from faust.serializers import codecs
from faust.types.models import ModelT
from faust.utils.compat import want_bytes
from faust.utils.futures import done_future

TEST_TOPIC = 'test'


class Key(faust.Record):
    value: int


class Value(faust.Record, serializer='json'):
    amount: float


@pytest.fixture
def app():
    instance = faust.App('testid')
    instance.producer = Mock(name='producer')
    return instance


def setup_producer(app):
    app.producer.maybe_start.return_value = done_future()
    app.producer.start.return_value = done_future()
    app.producer.send.return_value = done_future()
    app.producer.send_and_wait.return_value = done_future()


@pytest.mark.asyncio
@pytest.mark.parametrize('key,wait,topic_name,expected_topic,key_serializer', [
    ('key', True, TEST_TOPIC, TEST_TOPIC, None),
    (Key(value=10), True, TEST_TOPIC, TEST_TOPIC, None),
    ({'key': 'k'}, True, TEST_TOPIC, TEST_TOPIC, 'json'),
    (None, True, 'topic', 'topic', None),
    (b'key', False, TEST_TOPIC, TEST_TOPIC, None),
    ('key', False, 'topic', 'topic', None),
])
async def test_send(
        key, wait, topic_name, expected_topic, key_serializer, app):
    topic = app.topic(topic_name)
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
            expected_key = key.dumps(serializer='json')
        elif key_serializer:
            expected_key = codecs.dumps(key_serializer, key)
        else:
            expected_key = want_bytes(key)
    else:
        expected_key = None
    expected_sender.assert_called_with(
        expected_topic, expected_key, event.dumps(), partition=None,
    )


def test_stream(app):
    s = app.topic(TEST_TOPIC).stream()
    assert s.source.topic.topics == (TEST_TOPIC,)
    assert s.source in app.sources
    assert s.source.app == app


@pytest.mark.asyncio
async def test_stream_with_coroutine(app):

    async def coro(it):
        ...

    s = app.topic(TEST_TOPIC).stream(coro)
    assert s._coroutine


@pytest.mark.asyncio
async def test_on_stop_producer(app):
    app._service._active_children.append(app._producer)
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
