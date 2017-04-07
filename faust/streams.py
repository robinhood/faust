"""Streams."""
import asyncio
import faust
import re
import reprlib
from collections import defaultdict
from typing import (
    Any, AsyncIterator, Awaitable, Callable, Dict, List,
    Mapping, MutableMapping, MutableSequence, Pattern,
    Set, Sequence, Tuple, Type, Union, cast
)
from . import joins
from .types import AppT, CodecArg, K, Message, Topic, TopicPartition
from .types.transports import ConsumerCallback, ConsumerT
from .types.coroutines import CoroCallbackT
from .types.joins import JoinT
from .types.models import Event, FieldDescriptorT
from .types.streams import (
    Processor, StreamCoroutine, StreamCoroutineMap,
    StreamProcessorMap, StreamT, StreamManagerT,
)
from .utils.aiter import aenumerate
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
#   - The fact that a Stream can consume from multiple Topic descriptions is
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

    _processors: MutableMapping[Topic, MutableSequence[Processor]] = None
    _coroutines: StreamCoroutineMap = None
    _topicmap: MutableMapping[str, Topic] = None

    @classmethod
    def from_topic(cls, topic: Topic = None,
                   *,
                   coroutine: StreamCoroutine = None,
                   processors: Sequence[Processor] = None,
                   loop: asyncio.AbstractEventLoop = None,
                   **kwargs: Any) -> StreamT:
        return cls(
            topics=[topic] if topic is not None else [],
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
                 on_start: Callable = None,
                 join_strategy: JoinT = None,
                 app: AppT = None,
                 active: bool = True,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        # WARNING: App might be None here, only use the app in .bind, .on_bind
        self.app = app
        self.name = name
        if not isinstance(topics, MutableSequence):
            topics = list(topics)
        self.topics = cast(MutableSequence, topics)
        self.active = active
        self._processors = {}
        if processors:
            # Convert immutable processor list to mutable lists.
            for _topic, _processors in processors.items():
                if not isinstance(_processors, MutableSequence):
                    _processors = list(_processors)
                self._processors[_topic] = _processors
        self._coroutines = coroutines or {}
        self._on_start = on_start
        self.join_strategy = join_strategy
        self.children = children if children is not None else []
        self.outbox = asyncio.Queue(maxsize=1, loop=self.loop)
        if self.topics:
            self._topicmap = self._build_topicmap(self.topics)
        else:
            self._topicmap = {}
        self._on_message = self._compile_message_handler()
        Service.__init__(self, loop=loop)

    def bind(self, app: AppT) -> StreamT:
        """Create a new clone of this stream that is bound to an app."""
        return self.clone()._bind(app)

    def _bind(self, app: AppT) -> StreamT:
        """Bind this stream to specific app."""
        self.app = app
        self.name = app.new_stream_name()
        app.add_source(self)
        self.on_bind(app)
        return self

    def on_bind(self, app: AppT) -> None:
        ...

    def add_processor(self, processor: Processor,
                      *,
                      topics: Sequence[Topic] = None) -> None:
        # adds to all topics by default.
        if topics is None:
            topics = self.topics
        for topic in topics:
            self._add_processor_to_topic(topic, processor)

    def _add_processor_to_topic(self,
                                topic: Topic, processor: Processor) -> None:
        try:
            procs = self._processors[topic]
        except KeyError:
            procs = self._processors[topic] = []
        procs.append(processor)

    def info(self) -> Mapping[str, Any]:
        return {
            'app': self.app,
            'name': self.name,
            'topics': self.topics,
            'processors': self._processors,
            'coroutines': self._coroutines,
            'on_start': self._on_start,
            'loop': self.loop,
            'children': self.children,
            'active': self.active,
        }

    def clone(self, **kwargs: Any) -> StreamT:
        s = self.__class__(**{**self.info(), **kwargs})
        if self.app:
            return s._bind(self.app)  # bind new stream to app
        return s

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

    async def items(self) -> AsyncIterator[Tuple[K, Event]]:
        async for event in self:
            yield event.req.key, event

    def tee(self, n: int = 2) -> Tuple[StreamT, ...]:
        streams = [
            self.clone(active=False, on_start=self.maybe_start)
            for _ in range(n)
        ]

        async def forwarder(event: Event) -> Event:
            for stream in streams:
                await stream.put_event(event)
            return event
        self.add_processor(forwarder)
        return tuple(streams)

    def through(self, topic: Union[str, Topic]) -> StreamT:
        if isinstance(topic, str):
            topic = self.derive_topic(topic)
        topic = cast(Topic, topic)

        async def forwarder(event: Event) -> Event:
            await event.forward(topic)
            return event
        self.add_processor(forwarder)
        return self.clone(topics=[topic], on_start=self.maybe_start)

    def derive_topic(self, name: str) -> Topic:
        # find out the key_type/value_type from topic in this stream
        # make sure it's the same for all topics.
        key_type, value_type = self._get_uniform_topic_type()
        return faust.topic(
            name,
            key_type=key_type,
            value_type=value_type,
        )

    def _get_uniform_topic_type(self):
        key_type: Type = None
        value_type: Type = None
        for topic in self.topics:
            if key_type is None:
                key_type = topic.key_type
            else:
                assert topic.key_type is key_type
            if value_type is None:
                value_type = topic.value_type
                assert topic.value_type is value_type
        return key_type, value_type

    def enumerate(self,
                  start: int = 0) -> AsyncIterator[Tuple[int, Event]]:
        return aenumerate(self, start)

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

        async def on_message(message: Message) -> None:
            k = v = None
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
                await coroutine.send(v, on_done)
            else:
                await on_done(v)
        return on_message

    async def put_event(self, value: Event) -> None:
        topic = self._topicmap[value.req.message.topic]
        processors = self._processors.get(topic)
        value = await self.process(value.req.key, value)
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
        else:
            await self.on_done(value)

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
        if not isinstance(processors, MutableSequence):
            processors = list(processors)
        self._processors[topic] = processors
        self._coroutines[topic] = wrap_callback(coroutine, loop=self.loop)
        await self.app.streams.update()

    async def unsubscribe(self, topic: Topic) -> None:
        try:
            self.topics.remove(topic)
        except ValueError:
            pass
        self._processors.pop(topic, None)
        self._coroutines.pop(topic, None)
        await self.app.streams.update()

    async def on_start(self) -> None:
        if self.app is None:
            raise RuntimeError('Cannot start stream not bound to app.')
        if self._on_start:
            await self._on_start()

    async def on_stop(self) -> None:
        for coroutine in self._coroutines.values():
            await coroutine.join()

    async def on_key_decode_error(
            self, exc: Exception, message: Message) -> None:
        logger.error('Cannot decode key: %r: %r', message.key, exc)

    async def on_value_decode_error(
            self, exc: Exception, message: Message) -> None:
        logger.error('Cannot decode value for key=%r (%r): %r',
                     message.key, message.value, exc)

    def __and__(self, other: StreamT) -> StreamT:
        return self.combine(self, other)

    def __copy__(self) -> StreamT:
        return self.clone()

    def __iter__(self) -> Any:
        return self

    def __next__(self) -> Event:
        raise NotImplementedError('Streams are asynchronous: use __aiter__')

    async def __aiter__(self):
        await self.maybe_start()
        return self

    async def __anext__(self) -> Event:
        return cast(Event, await self.outbox.get())

    def _build_topicmap(
            self, topics: Sequence[Topic]) -> MutableMapping[str, Topic]:
        return {
            s: topic
            for topic in topics for s in self._subtopics_for(topic)
        }

    def _subtopics_for(self, topic: Topic) -> Sequence[str]:
        return topic.pattern.pattern if topic.pattern else topic.topics

    def _repr_info(self) -> str:
        if self.children:
            return reprlib.repr(self.children)
        elif len(self.topics) == 1:
            return reprlib.repr(self.topics[0])
        return reprlib.repr(self.topics)


class StreamManager(StreamManagerT, Service):

    #: Fast index to see if stream is registered.
    _streams: Set[StreamT]

    #: Map str topic to set of streams that should get a copy
    #: of each message sent to that topic.
    _topicmap: MutableMapping[str, Set[StreamT]]

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        self.app = app
        self.consumer = None
        self._streams = set()
        self._topicmap = defaultdict(set)
        super().__init__(**kwargs)

    def add_stream(self, stream: StreamT):
        if stream in self._streams:
            raise ValueError('Stream already registered with app')
        self._streams.add(stream)

    async def update(self):
        self._compile_pattern()
        await self.consumer.subscribe(self._pattern)

    def _compile_message_handler(self) -> ConsumerCallback:
        # topic str -> list of Stream
        get_streams_for_topic = self._topicmap.__getitem__

        async def on_message(message: Message) -> None:
            for stream in get_streams_for_topic(message.topic):
                await stream._on_message(message)  # type: ignore
        return on_message

    async def on_start(self) -> None:
        asyncio.ensure_future(self._delayed_start(), loop=self.loop)

    async def _delayed_start(self) -> None:
        # wait for tasks to start streams
        await asyncio.sleep(2.0, loop=self.loop)

        # then register topics etc.
        self._compile_pattern()
        self._on_message = self._compile_message_handler()
        self.consumer = self._create_consumer()
        await self.consumer.subscribe(self._pattern)
        await self.consumer.start()

    async def on_stop(self) -> None:
        if self.consumer:
            await self.consumer.stop()

    def _create_consumer(self) -> ConsumerT:
        return self.app.transport.create_consumer(
            callback=self._on_message,
            on_partitions_revoked=self._on_partitions_revoked,
            on_partitions_assigned=self._on_partitions_assigned
        )

    def _compile_pattern(self):
        self._topicmap.clear()
        for stream in self._streams:
            if stream.active:
                for topic in stream._topicmap:
                    self._topicmap[topic].add(stream)
        self._pattern = '|'.join(self._topicmap)

    def _on_partitions_assigned(self,
                                assigned: Sequence[TopicPartition]) -> None:
        ...

    def _on_partitions_revoked(self,
                               revoked: Sequence[TopicPartition]) -> None:
        ...
