import asyncio
import re
import pytest
from faust import Event, Record
from faust.exceptions import ValueDecodeError
from faust.types import Message
from mode.utils.mocks import AsyncMock, Mock


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
        return Mock(name='message', value=None, autospec=Message)

    def test_on_published(self, *, topic):
        fut = Mock(name='fut', autospec=asyncio.Future)
        message = Mock(name='message', autospec=Message)
        topic._on_published(fut, message)
        fut.result.assert_called_once_with()
        message.set_result.assert_called_once_with(fut.result())
        message.message.callback.assert_called_once_with(message)
        message.message.callback = None
        topic._on_published(fut, message)

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
    async def test_decode_empty_error(self, *, topic, message_empty_value):
        topic = topic.derive_topic(value_serializer='json', value_type=Dummy)
        with pytest.raises(ValueDecodeError):
            await topic.decode(message_empty_value, propagate=True)

    @pytest.mark.asyncio
    async def test_decode_allow_empty(self, *, topic_allow_empty,
                                      message_empty_value):
        event = await topic_allow_empty.decode(message_empty_value)
        assert event.value is None
        assert event.message == message_empty_value

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
