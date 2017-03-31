"""Streams."""
import asyncio
import re
import reprlib
from collections import OrderedDict
from typing import (
    Any, AsyncIterator, Awaitable, Dict, List,
    Mapping, MutableMapping, MutableSequence, Pattern,
    Sequence, Tuple, Type, Union, cast
)
from . import joins
from . import primitives
from .types import AppT, CodecArg, K, Message, Topic
from .types.transports import ConsumerCallback, ConsumerT
from .types.coroutines import CoroCallbackT
from .types.joins import JoinT
from .types.models import Event, FieldDescriptorT
from .types.streams import (
    Processor, StreamCoroutine, StreamCoroutineMap,
    StreamProcessorMap, StreamT,
)
from .utils.coroutines import wrap_callback
from .utils.logging import get_logger
from .utils.services import Service

__all__ = ['Stream', 'topic']

__make_flake8_happy_List: List  # XXX flake8 thinks this is unused
__make_flake8_happy_Dict: Dict
__make_flake8_happy_CoroCallbackT: CoroCallbackT

logger = get_logger(__name__)

# NOTES:
#   - Users define an Record subclass that define how messages in a topic is
#     serialized/deserialized.
#
#       class Withdrawal(Record, serializer='json'):
#           account_id: str
#           amount: float
#
#   - Users create a topic description: Topic, that describes a list of
#     topics and the Record class used to serialize/deserialize messages:
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
#            _processors: MutableMapping[Topic, Callable[[V], V]]
#
#   - A processor can either be a regular callable, or an async callable:
#
#       # NOTE: Event is the type of  a ModelT (Record/etc.) that was
#       #       received as a message
#
#       def processor1(event: Event) -> Event:
#           return event.amount * 2
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
          key_type: Type = None,
          value_type: Type = None,
          key_serializer: CodecArg = None) -> Topic:
    """Define new topic.

    Arguments:
        *topics: str:  List of topic names.

    Keyword Arguments:
        pattern (Union[str, Pattern]): Regular expression to match.
            You cannot specify both topics and a pattern.
        key_type (Type): Model used for keys in this topic.
        value_type (Type): Model used for values in this topic.

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
        key_type=key_type,
        value_type=value_type,
    )


class Stream(StreamT, Service):

    _consumers: MutableMapping[Topic, ConsumerT] = None
    _processors: StreamProcessorMap = None
    _coroutines: StreamCoroutineMap = None

    @classmethod
    def from_topic(cls, topic: Topic,
                   *,
                   coroutine: StreamCoroutine = None,
                   processors: Sequence[Processor] = None,
                   loop: asyncio.AbstractEventLoop = None,
                   **kwargs: Any) -> StreamT:
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

    def __init__(self, *,
                 name: str = None,
                 topics: Sequence[Topic] = None,
                 processors: StreamProcessorMap = None,
                 coroutines: StreamCoroutineMap = None,
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
        self.outbox = asyncio.Queue(maxsize=1, loop=self.loop)
        self._on_message: ConsumerCallback = None
        super().__init__(loop=loop)

    def bind(self, app: AppT) -> StreamT:
        """Create a new clone of this stream that is bound to an app."""
        stream = self.clone(name=app.new_stream_name(), app=app)
        app.add_source(stream)
        self.on_bind(app)
        return stream

    def on_bind(self, app: AppT) -> None:
        ...

    def info(self) -> Mapping[str, Any]:
        return {
            'name': self.name,
            'topics': self.topics,
            'processors': self._processors,
            'coroutines': self._coroutines,
            'loop': self.loop,
            'children': self.children,
        }

    def clone(self, **kwargs: Any) -> StreamT:
        return self.__class__(**{**self.info(), **kwargs})

    def combine(self, *nodes: StreamT, **kwargs: Any) -> StreamT:
        all_nodes = cast(Tuple[StreamT, ...], (self,)) + nodes
        topics: List[Topic] = []
        processors: Dict[Topic, Sequence[Processor]] = {}
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

    async def through(self, topic: Union[str, Topic]) -> AsyncIterator[Event]:
        return primitives.through(self, topic)

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

    def _compile_message_handler(self) -> ConsumerCallback:
        # topic str -> Topic description
        get_topic = self._topicmap.__getitem__
        # Topic description -> processors
        get_processors = self._processors.get
        # Topic description -> special coroutine
        get_coroutines = self._coroutines.get
        # deserializing keys/values
        loads_key = self.app.loads_key
        loads_value = self.app.loads_value
        # .process() coroutine
        process = self.process
        # .on_done callback
        on_done = self.on_done

        async def on_message(consumer: ConsumerT, message: Message) -> None:
            topic = get_topic(message.topic)
            try:
                k = await loads_key(topic.key_type, message.key)
            except Exception as exc:
                await self.on_key_decode_error(exc, message)
            else:
                try:
                    v = await loads_value(topic.value_type, k, message)
                except Exception as exc:
                    await self.on_value_decode_error(exc, message)
                else:
                    consumer.track_event(v, message.offset)
            processors = get_processors(topic)
            v = await process(k, v)
            if processors is not None:
                for processor in processors:
                    res = processor(v)
                    if isinstance(res, Awaitable):
                        v = await res
                    else:
                        v = res
            coroutine = get_coroutines(topic)
            if coroutine is not None:
                await coroutine.send(v, self.on_done)
            else:
                await on_done(v)
        return on_message

    async def process(self, key: K, value: Event) -> Event:
        return value

    async def on_done(self, value: Event = None) -> None:
        join_strategy = self.join_strategy
        if join_strategy:
            value = await join_strategy.process(value)
        if value is not None:
            outbox = self.outbox
            if outbox:
                await outbox.put(value)

    async def subscribe(self, topic: Topic,
                        *,
                        processors: Sequence[Processor] = None,
                        coroutine: StreamCoroutine = None) -> None:
        if topic not in self.topics:
            self.topics.append(topic)
        self._processors[topic] = processors
        self._coroutines[topic] = wrap_callback(coroutine, loop=self.loop)
        await self._update_subscription()

    async def unsubscribe(self, topic: Topic) -> None:
        try:
            self.topics.remove(topic)
        except ValueError:
            pass
        self._processors.pop(topic, None)
        self._coroutines.pop(topic, None)
        await self._update_subscription()

    async def on_start(self) -> None:
        if self.app is None:
            raise RuntimeError('Cannot start stream not bound to app.')
        self._compile_pattern()
        self._on_message = self._compile_message_handler()
        self._consumer = self._create_consumer()
        await self._consumer.subscribe(self._pattern)
        await self._consumer.start()

    async def _update_subscription(self):
        self._compile_pattern()
        await self._consumer.subscribe(self._pattern)

    async def on_stop(self) -> None:
        for consumer in reversed(list(self._consumers.values())):
            await consumer.stop()
        self._consumers.clear()
        for coroutine in self._coroutines.values():
            await coroutine.join()

    async def on_key_decode_error(
            self, exc: Exception, message: Message) -> None:
        logger.error('Cannot decode key: %r: %r', message.key, exc)

    async def on_value_decode_error(
            self, exc: Exception, message: Message) -> None:
        logger.error('Cannot decode value for key=%r (%r): %r',
                     message.key, message.value, exc)

    def _create_consumer(self) -> ConsumerT:
        return self.app.transport.create_consumer(callback=self._on_message)

    def _compile_pattern(self):
        self._topicmap = self._build_topicmap(self.topics)
        self._pattern = '|'.join(self._topicmap)

    def _build_topicmap(
            self, topics: Sequence[Topic]) -> Mapping[str, Topic]:
        return {
            s: topic
            for topic in topics for s in self._subtopics_for(topic)
        }

    def _subtopics_for(self, topic: Topic) -> Sequence[str]:
        return topic.pattern.pattern if topic.pattern else topic.topics

    def __and__(self, other: StreamT) -> StreamT:
        return self.combine(self, other)

    def __copy__(self) -> StreamT:
        return self.clone()

    def __iter__(self) -> Any:
        return self

    def __next__(self) -> Event:
        raise NotImplementedError('Stream are asynchronous use __aiter__')

    async def __aiter__(self):
        await self.maybe_start()
        return self

    async def __anext__(self) -> Event:
        return cast(Event, await self.outbox.get())

    def _repr_info(self) -> str:
        if self.children:
            return reprlib.repr(self.children)
        elif len(self.topics) == 1:
            return reprlib.repr(self.topics[0])
        return reprlib.repr(self.topics)
