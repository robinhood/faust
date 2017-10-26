from unittest.mock import ANY, Mock
import faust
from faust.serializers import codecs
from faust.types.models import ModelT
from faust.utils.compat import want_bytes
from faust.utils.futures import done_future
import pytest

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
@pytest.mark.parametrize('key,topic_name,expected_topic,key_serializer', [
    ('key', TEST_TOPIC, TEST_TOPIC, None),
    (Key(value=10), TEST_TOPIC, TEST_TOPIC, None),
    ({'key': 'k'}, TEST_TOPIC, TEST_TOPIC, 'json'),
    (None, 'topic', 'topic', None),
    (b'key', TEST_TOPIC, TEST_TOPIC, None),
    ('key', 'topic', 'topic', None),
])
async def test_send(
        key, topic_name, expected_topic, key_serializer, app):
    topic = app.topic(topic_name)
    event = Value(amount=0.0)
    setup_producer(app)
    await app.send(topic, key, event, key_serializer=key_serializer)
    # do it twice so producer_started is also True
    await app.send(topic, key, event, key_serializer=key_serializer)
    expected_sender = app.producer.send_and_wait
    if key is not None:
        if isinstance(key, str):
            # Default serializer is json, and str should be serialized
            # (note that a bytes key will be left alone.
            key_serializer = 'json'
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
    assert s.channel.topics == (TEST_TOPIC,)
    assert s.channel in app.topics
    assert s.channel.app == app


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


def test_new_transport(app, patching):
    by_url = patching('faust.transport.by_url')
    assert app._new_transport() is by_url.return_value.return_value
    assert app.transport is by_url.return_value.return_value
    by_url.assert_called_with(app.url)
    by_url.return_value.assert_called_with(app.url, app, loop=app.loop)
    app.transport = 10
    assert app.transport == 10
