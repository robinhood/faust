import asyncio
import re
import pytest
from faust import Event, Record
from faust.exceptions import KeyDecodeError, ValueDecodeError
from faust.types import Message
from mode.utils.mocks import AsyncMock, Mock, call, patch


class Dummy(Record):
    foo: int


class test_Topic:

    @pytest.fixture
    def topic(self, *, app):
        return app.topic('foo')

    @pytest.fixture
    def topic_allow_empty(self, *, app):
        return app.topic('foo', allow_empty=True)

    @pytest.fixture
    def message(self):
        return Mock(name='message', autospec=Message)

    @pytest.fixture
    def message_empty_value(self):
        return Mock(name='message', value=None, headers=[], autospec=Message)

    def test_init_key_serializer_taken_from_key_type(self, app):
        class M(Record, serializer='foobar'):
            x: int

        topic = app.topic('foo', key_type=M, value_type=M)
        assert topic.key_serializer == 'foobar'
        assert topic.value_serializer == 'foobar'

    @pytest.mark.asyncio
    async def test_publish_message__wait_enabled(self, *, topic, app):
        producer = Mock(send_and_wait=AsyncMock())
        topic._get_producer = AsyncMock(return_value=producer)
        callback = Mock(name='callback')
        fm = topic.as_future_message(
            key='foo',
            value='bar',
            partition=130,
            timestamp=312.5134,
            headers={'k': 'v'},
            key_serializer='json',
            value_serializer='json',
            callback=callback,
        )
        await topic.publish_message(fm, wait=True)
        producer.send_and_wait.coro.assert_called_once_with(
            topic.get_topic_name(),
            topic.prepare_key('foo', 'json'),
            topic.prepare_value('bar', 'json'),
            headers={'k': 'v'},
            partition=130,
            timestamp=312.5134,
        )
        callback.assert_called_once_with(fm)

    @pytest.mark.asyncio
    async def test_send__attachments_enabled(self, *, topic, app):
        app._attachments.enabled = True
        callback = Mock(name='callback')
        with patch('faust.topics.current_event') as current_event:
            await topic.send(
                key='k',
                value='v',
                partition=3,
                timestamp=312.41,
                headers={'k': 'v'},
                key_serializer='foo',
                value_serializer='bar',
                callback=callback,
                force=False,
            )
            current_event.assert_called_once_with()
            current_event()._attach.assert_called_once_with(
                topic,
                'k',
                'v',
                partition=3,
                timestamp=312.41,
                headers={'k': 'v'},
                key_serializer='foo',
                value_serializer='bar',
                callback=callback,
            )

    @pytest.mark.asyncio
    async def test_send__attachments_no_event(self, *, topic, app):
        app._attachments.enabled = True
        callback = Mock(name='callback')
        topic._send_now = AsyncMock()
        with patch('faust.topics.current_event', Mock(return_value=None)):
            await topic.send(
                key='k',
                value='v',
                partition=3,
                timestamp=312.41,
                headers={'k': 'v'},
                key_serializer='foo',
                value_serializer='bar',
                callback=callback,
                force=False,
            )
            topic._send_now.assert_called_once_with(
                'k',
                'v',
                partition=3,
                timestamp=312.41,
                headers={'k': 'v'},
                key_serializer='foo',
                value_serializer='bar',
                callback=callback,
            )

    def test_on_published(self, *, topic, app):
        app.sensors.on_send_completed = Mock(name='on_send_completed')
        producer = Mock(name='producer')
        state = Mock(name='state')
        fut = Mock(name='fut', autospec=asyncio.Future)
        message = Mock(name='message', autospec=Message)
        topic._on_published(fut, message, producer, state)
        fut.result.assert_called_once_with()
        app.sensors.on_send_completed.assert_called_once_with(
            producer, state, fut.result())
        message.set_result.assert_called_once_with(fut.result())
        message.message.callback.assert_called_once_with(message)
        message.message.callback = None
        topic._on_published(fut, message, producer, state)

    def test_on_published__error(self, *, topic, app):
        app.sensors.on_send_error = Mock(name='on_send_error')
        producer = Mock(name='producer')
        state = Mock(name='state')
        fut = Mock(name='fut', autospec=asyncio.Future)
        exc = fut.result.side_effect = KeyError()
        message = Mock(name='message', autospec=Message)
        topic._on_published(fut, message, producer, state)

        message.set_exception.assert_called_once_with(exc)
        app.sensors.on_send_error.assert_called_once_with(
            producer, exc, state,
        )

    def test_aiter_when_iterator(self, *, topic):
        topic.is_iterator = True
        assert topic.__aiter__() is topic

    @pytest.mark.asyncio
    async def test_decode(self, *, topic, message):
        topic._compile_decode = Mock(name='_compile_decode')
        topic._compile_decode.return_value = AsyncMock()

        await topic.decode(message, propagate=True)
        topic._compile_decode.assert_called_once_with()

    @pytest.mark.asyncio
    async def test_decode__decode_error_propagate(self, *, topic, message):
        exc = KeyDecodeError()
        topic.app.serializers.loads_key = Mock(side_effect=exc)
        with pytest.raises(type(exc)):
            await topic.decode(message, propagate=True)

    @pytest.mark.asyncio
    async def test_decode__decode_error_callback(self, *, topic, message):
        exc = KeyDecodeError()
        topic.app.serializers.loads_key = Mock(side_effect=exc)
        topic.on_key_decode_error = AsyncMock()
        await topic.decode(message, propagate=False)
        topic.on_key_decode_error.assert_called_once_with(exc, message)

    @pytest.mark.asyncio
    async def test_derive(self, *, topic):
        t = topic.derive(key_serializer='raw')
        assert t.key_serializer == 'raw'
        assert t.value_serializer == topic.value_serializer
        assert t.topics == topic.topics

    @pytest.mark.asyncio
    async def test_derive__prefix(self, *, topic):
        topic.topics = ['foo', 'bar']
        t = topic.derive(key_serializer='raw', prefix='foo_')
        assert t.key_serializer == 'raw'
        assert t.value_serializer == topic.value_serializer
        assert t.topics == ['foo_foo', 'foo_bar']

    @pytest.mark.asyncio
    async def test_derive__with_pattern(self, *, topic):
        topic.topics = None
        topic.pattern = r'.*'
        with pytest.raises(ValueError):
            topic.derive(prefix='foo')

    @pytest.mark.asyncio
    async def test_decode_empty_error(self, *, topic, message_empty_value):
        topic = topic.derive_topic(value_serializer='json', value_type=Dummy)
        with pytest.raises(ValueDecodeError):
            await topic.decode(message_empty_value, propagate=True)

    @pytest.mark.asyncio
    async def test_decode_empty_error_callback(
            self, *, topic, message_empty_value):
        topic = topic.derive_topic(value_serializer='json', value_type=Dummy)
        topic.on_value_decode_error = AsyncMock()
        await topic.decode(message_empty_value, propagate=False)
        topic.on_value_decode_error.assert_called_once()

    @pytest.mark.asyncio
    async def test_decode_allow_empty(self, *, topic_allow_empty,
                                      message_empty_value):
        event = await topic_allow_empty.decode(message_empty_value)
        assert event.value is None
        assert event.message == message_empty_value

    def test__topic_name_or_default__str(self, *, topic):
        assert topic._topic_name_or_default('xyz') == 'xyz'

    def test__topic_name_or_default__default(self, *, topic):
        assert topic._topic_name_or_default(None) == topic.get_topic_name()

    def test__topic_name_or_default__channel(self, *, topic, app):
        t2 = app.topic('bar')
        assert topic._topic_name_or_default(t2) == 'bar'

    @pytest.mark.asyncio
    async def test_maybe_declare(self, *, topic):
        topic.declare = AsyncMock()
        await asyncio.gather(
            topic.maybe_declare(),
            topic.maybe_declare(),
            topic.maybe_declare(),
            topic.maybe_declare(),
            topic.maybe_declare(),
            topic.maybe_declare(),
        )
        topic.declare.assert_called_once_with()
        await asyncio.gather(
            topic.maybe_declare(),
            topic.maybe_declare(),
            topic.maybe_declare(),
            topic.maybe_declare(),
            topic.maybe_declare(),
            topic.maybe_declare(),
        )
        assert topic.declare.call_count == 2

    @pytest.mark.asyncio
    async def test_declare__disabled(self, *, topic):
        topic.app.conf.topic_allow_declare = False
        producer = Mock(create_topic=AsyncMock())
        topic._get_producer = AsyncMock(return_value=producer)
        topic.partitions = 101
        topic.replicas = 202
        topic.topics = ['foo', 'bar']
        await topic.declare()
        producer.create_topic.coro.assert_not_called()

    @pytest.mark.asyncio
    async def test_declare(self, *, topic):
        topic.app.conf.topic_allow_declare = True
        producer = Mock(create_topic=AsyncMock())
        topic._get_producer = AsyncMock(return_value=producer)
        topic.partitions = 101
        topic.replicas = 202
        topic.topics = ['foo', 'bar']
        await topic.declare()
        producer.create_topic.coro.assert_has_calls([
            call(
                topic='foo',
                partitions=101,
                replication=202,
                config=topic.config,
                compacting=topic.compacting,
                deleting=topic.deleting,
                retention=topic.retention,
            ),
            call(
                topic='bar',
                partitions=101,
                replication=202,
                config=topic.config,
                compacting=topic.compacting,
                deleting=topic.deleting,
                retention=topic.retention,
            ),
        ])

    @pytest.mark.asyncio
    async def test_declare__defaults(self, *, topic):
        topic.app.conf.topic_allow_declare = True
        producer = Mock(create_topic=AsyncMock())
        topic._get_producer = AsyncMock(return_value=producer)
        topic.partitions = None
        topic.replicas = None
        topic.topics = ['foo', 'bar']
        await topic.declare()
        producer.create_topic.coro.assert_has_calls([
            call(
                topic='foo',
                partitions=topic.app.conf.topic_partitions,
                replication=topic.app.conf.topic_replication_factor,
                config=topic.config,
                compacting=topic.compacting,
                deleting=topic.deleting,
                retention=topic.retention,
            ),
            call(
                topic='bar',
                partitions=topic.app.conf.topic_partitions,
                replication=topic.app.conf.topic_replication_factor,
                config=topic.config,
                compacting=topic.compacting,
                deleting=topic.deleting,
                retention=topic.retention,
            ),
        ])

    @pytest.mark.asyncio
    async def test_put(self, *, topic):
        topic.is_iterator = True
        topic.queue.put = AsyncMock(name='queue.put')
        event = Mock(name='event', autospec=Event)
        await topic.put(event)
        topic.queue.put.assert_called_once_with(event)

    @pytest.mark.asyncio
    async def test_put__raise_when_not_iterator(self, *, topic):
        topic.is_iterator = False
        with pytest.raises(RuntimeError):
            await topic.put(Mock(name='event', autospec=Event))

    def test_set_pattern__raise_when_topics(self, *, topic):
        topic.topics = ['A', 'B']
        with pytest.raises(TypeError):
            topic.pattern = re.compile('something.*')

    def test_set_partitions__raise_when_zero(self, *, topic):
        with pytest.raises(ValueError):
            topic.partitions = 0

    def test_derive_topic__raise_when_no_sub(self, *m, topic):
        topic.topics = None
        topic.pattern = None
        with pytest.raises(TypeError):
            topic.get_topic_name()

    def test_derive_topic__raise_if_pattern_and_prefix(self, *, topic):
        topic.topics = None
        topic.pattern = re.compile('something2.*')
        with pytest.raises(ValueError):
            topic.derive_topic(suffix='-repartition')

    def test_get_topic_name__raise_when_pattern(self, *, topic):
        topic.topics = None
        topic.pattern = re.compile('^foo.$')
        with pytest.raises(TypeError):
            topic.get_topic_name()

    def test_get_topic_name__raise_if_multitopic(self, *, topic):
        topic.topics = ['t1', 't2']
        with pytest.raises(ValueError):
            topic.get_topic_name()
