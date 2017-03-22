"""Streams."""
import asyncio
import re
from collections import OrderedDict
from typing import (
    Any, AsyncIterable, Awaitable, Callable, Dict, List,
    Mapping, MutableMapping, MutableSequence, Pattern,
    Sequence, Tuple, Type, Union, cast
)
from . import joins
from .types import (
    AppT, ConsumerT, CoroCallbackT, FieldDescriptorT, JoinT, K, V,
    Message, SerializerArg, StreamT, Topic,
)
from .utils.coroutines import wrap_callback
from .utils.log import get_logger
from .utils.service import Service

__all__ = ['AsyncIterableStream', 'Stream', 'topic']

__make_flake8_happy_List: List  # XXX flake8 thinks this is unused
__make_flake8_happy_Dict: Dict

logger = get_logger(__name__)

# NOTES:
#   - Users define an Event subclass that define how messages in a topic is
#     serialized/deserialized.
#
#       class Withdrawal(Event, serializer='json'):
#           account_id: str
#           amount: float
#
#   - Users create a topic description: Topic, that describes a list of
#     topics and the Event class used to serialize/deserialize messages:
#
#       # topic is a shortcut function that returns type faust.types.Topic
#       withdrawals = faust.topic('withdrawal.ach', 'withdrawal.paypal',
#                                 type=Withdrawal)
#
#   - A Stream can subscribe to multiple Topic descriptions, and it can have
#     a chain of processors for each topic:
#
#        class Stream:
#            topics: Sequence[Topic]
#            _processors: MutableMapping[Topic, Callable]
#
#   - A processor can either be a regular callable, or an async callable:
#
#       def processor1(event: Event) -> Event:
#           return Event.amount * 2
#
#       async def processor2(event: Event) -> Event:
#           await verify_event(event)
#           return event
#
#       s = Stream(
#           topics=[withdrawals],
#           processors={
#               withdrawals: [processor1, processor2],
#           },
#       )
#
#   - The Stream above is currently not associated with an App, and cannot
#     be started yet.  To do so you need to bind it to an app:
#
#        bound_s = s.bind(app)
#
#   - Users will usually not instantiate Stream directly, instead they will
#     use the app to create streams, this will also take care of binding:
#
#       s = app.stream(withdrawals)
#
#   - In this app.stream signature you see that the stream only accepts a
#     single Topic description
#
#   - The fact that a Stream can consume from multiple Topic description is
#     an internal detail for the implementation of joins:
#
#      # Two streams can be combined:
#      combined_s = (s1 & s2)
#      # Iterating over this stream will give events from both streams:
#      for event in combined_s:
#          ...
#
#      A combined stream can also specify a join strategy that decides how
#      events from the combined streams are joined together into a single
#      event:
#
#      for event in (s1 & s2).join(Withdrawal.account_id, Account.id):
#          ...


def topic(*topics: str,
          pattern: Union[str, Pattern] = None,
          type: Type = None,
          key_serializer: SerializerArg = None) -> Topic:
    """Define new topic.

    Arguments:
        *topics: str:  List of topic names.

    Keyword Arguments:
        pattern (Union[str, Pattern]): Regular expression to match.
            You cannot specify both topics and a pattern.
        type (Type): Event type used for messages in this topic.
        key_serializer (SerializerArg): Serializer name, or serializer object
            to use for keys from this topic.

    Raises:
        TypeError: if both `topics` and `pattern` is provided.

    Returns:
        faust.types.Topic: a named tuple.

    """
    if pattern and topics:
        raise TypeError('Cannot specify both topics and pattern.')
    if isinstance(pattern, str):
        pattern = re.compile(pattern)

    return Topic(
        topics=topics,
        pattern=pattern,
        type=type,
        key_serializer=key_serializer,
    )


class Stream(StreamT, Service):

    _consumers: MutableMapping[Topic, ConsumerT] = None
    _processors: MutableMapping[Topic, Sequence[Callable]] = None
    _coroutines: MutableMapping[Topic, CoroCallbackT] = None

    @classmethod
    def from_topic(cls, topic: Topic,
                   *,
                   coroutine: Callable = None,
                   processors: Sequence[Callable] = None,
                   loop: asyncio.AbstractEventLoop = None,
                   **kwargs) -> StreamT:
        return cls(
            topics=[topic],
            coroutines={
                topic: wrap_callback(coroutine, loop=loop),
            } if coroutine else None,
            processors={
                topic: processors,
            } if processors else None,
            loop=loop,
            **kwargs)

    def __init__(self, name: str = None,
                 topics: Sequence[Topic] = None,
                 processors: MutableMapping[Topic, Sequence[Callable]] = None,
                 coroutines: MutableMapping[Topic, CoroCallbackT] = None,
                 children: List[StreamT] = None,
                 join_strategy: JoinT = None,
                 app: AppT = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.app = app
        self.name = name
        if not isinstance(topics, MutableSequence):
            topics = list(topics)
        self.topics = cast(MutableSequence, topics)
        self._processors = processors or {}
        self._consumers = OrderedDict()
        self._coroutines = coroutines or {}
        self.join_strategy = join_strategy
        self.children = children if children is not None else []
        super().__init__(loop=loop)

    def bind(self, app: AppT) -> StreamT:
        """Create a new clone of this stream that is bound to an app."""
        stream = self.clone(name=app.new_stream_name(), app=app)
        app.add_source(stream)
        return stream

    def info(self) -> Mapping[str, Any]:
        return {
            'name': self.name,
            'topics': self.topics,
            'processors': self._processors,
            'coroutines': self._coroutines,
            'loop': self.loop,
            'children': self.children,
        }

    def clone(self, **kwargs) -> StreamT:
        return self.__class__(**{**self.info(), **kwargs})

    def combine(self, *nodes: StreamT, **kwargs):
        all_nodes = cast(Tuple[StreamT, ...], (self,)) + nodes
        topics: List[Topic] = []
        processors: Dict[Topic, Sequence[Callable]] = {}
        coroutines: Dict[Topic, CoroCallbackT] = {}
        for node in all_nodes:
            node = cast(Stream, node)
            topics.extend(node.topics)
            processors.update(node._processors)
            coroutines.update(node._coroutines)
        return self.clone(
            topics=topics,
            processors=processors,
            coroutines=coroutines,
            children=self.children + list(nodes),
        )

    def join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(joins.RightJoin(stream=self, fields=fields))

    def left_join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(joins.LeftJoin(stream=self, fields=fields))

    def inner_join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(joins.InnerJoin(stream=self, fields=fields))

    def outer_join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(joins.OuterJoin(stream=self, fields=fields))

    def _join(self, join_strategy: JoinT) -> StreamT:
        return self.clone(join_strategy=join_strategy)

    async def on_message(self, topic: Topic, key: K, value: V) -> None:
        processors = self._processors.get(topic)
        value = await self.process(key, value)
        if processors is not None:
            for processor in processors:
                res = processor(value)
                if isinstance(res, Awaitable):
                    value = await res
                else:
                    value = res
        coroutine = self._coroutines.get(topic)
        if coroutine is not None:
            await coroutine.send(value, self.on_done)

    async def process(self, key: K, value: V) -> V:
        return value

    async def on_done(self, value: V = None) -> None:
        join_strategy = self.join_strategy
        if join_strategy:
            value = await join_strategy.process(value)
        if value is not None:
            outbox = self.outbox
            if outbox:
                await outbox.put(value)

    async def subscribe(self, topic: Topic,
                        *,
                        processors: Sequence[Callable] = None,
                        coroutine: Callable = None) -> None:
        if topic not in self.topics:
            self.topics.append(topic)
        self._processors[topic] = processors
        self._coroutines[topic] = wrap_callback(coroutine, loop=self.loop)
        await self._subscribe(topic)

    async def _subscribe(self, topic: Topic) -> None:
        if topic not in self._consumers:
            c = self._consumers[topic] = self._create_consumer_for_topic(topic)
            await c.start()

    async def unsubscribe(self, topic: Topic) -> None:
        try:
            self.topics.remove(topic)
        except ValueError:
            pass
        self._processors.pop(topic, None)
        self._coroutines.pop(topic, None)
        await self._unsubscribe(topic)

    async def _unsubscribe(self, topic: Topic) -> None:
        try:
            consumer = self._consumers.pop(topic)
        except KeyError:
            pass
        else:
            await consumer.stop()

    async def on_start(self) -> None:
        if self.app is None:
            raise RuntimeError('Cannot start stream not bound to app.')
        for topic in self.topics:
            await self._subscribe(topic)

    async def on_stop(self) -> None:
        for consumer in reversed(list(self._consumers.values())):
            await consumer.stop()
        self._consumers.clear()
        for coroutine in self._coroutines.values():
            await coroutine.join()

    def on_key_decode_error(self, exc: Exception, message: Message) -> None:
        logger.error('Cannot decode key: %r: %r', message.key, exc)

    def on_value_decode_error(self, exc: Exception, message: Message) -> None:
        logger.error('Cannot decode value for key=%r (%r): %r',
                     message.key, message.value, exc)

    def _create_consumer_for_topic(self, topic: Topic) -> ConsumerT:
        return self.app.transport.create_consumer(
            topic=topic,
            callback=self.on_message,
            on_key_decode_error=self.on_key_decode_error,
            on_value_decode_error=self.on_value_decode_error,
        )

    def __and__(self, other: StreamT) -> StreamT:
        return self.combine(self, other)

    def __copy__(self) -> StreamT:
        return self.clone()


class AsyncIterableStream(Stream, AsyncIterable):

    def on_init(self) -> None:
        self.outbox = asyncio.Queue(maxsize=1, loop=self.loop)

    async def __aiter__(self) -> 'AsyncIterableStream':
        await self.maybe_start()
        return self

    async def __anext__(self) -> Awaitable:
        return await self.outbox.get()
