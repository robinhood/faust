"""Streams."""
import asyncio
import re
from collections import OrderedDict
from typing import (
    Any, AsyncIterable, Awaitable, Callable, Dict, List,
    Mapping, MutableMapping, MutableSequence, Pattern,
    Sequence, Tuple, Type, Union, cast
)
from . import join
from .types import (
    AppT, ConsumerT, FieldDescriptorT, JoinT, K, V,
    Message, SerializerArg, StreamT, Topic,
)
from .utils.coroutines import CoroCallback, wrap_callback
from .utils.log import get_logger
from .utils.service import Service

__make_flake8_happy_List: List  # XXX flake8 thinks this is unused
__make_flake8_happy_Dict: Dict

logger = get_logger(__name__)

# NOTES:
#   - The stream decorator only takes one Topic, but a Stream can
#     consume from multiple Topics internally, this is especially useful when
#     streams are combined.
#   - The stream decorator returns an unbound stream:
#      - unbound means the stream is not associated with an app, and cannot
#        actually be started yet.
#      - `s2 = app.add_stream(s)` binds the stream to the app, this clones the
#        instance so the original stream can be bound multiple times.


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


class stream:
    """Decorator for stream-expressions.

    Arguments:
        topic (Topic): Topic to subscribe to.

    Keyword Arguments:
        callbacks (Sequence[Callable]): Optional list of callbacks to execute
            when a message is used.  The callbacks will be applied in order,
            and the return value is chained.
        loop (asyncio.AbstractEventLoop): Custom event loop instance.

    Returns:
        Stream: an unbound stream.
    """

    def __init__(self, topic: Topic,
                 *,
                 callbacks: Sequence[Callable] = None,
                 loop: asyncio.AbstractEventLoop = None,
                 **kwargs) -> None:
        self.topic = topic
        self.callbacks = callbacks
        self.loop = loop or asyncio.get_event_loop()

    def __call__(self, fun: Callable) -> 'StreamT':
        return AsyncIterableStream(
            topics=[self.topic],
            callbacks={self.topic: self.callbacks},
            coros={self.topic: wrap_callback(fun, loop=self.loop)},
            loop=self.loop,
        )


class Stream(StreamT, Service):

    _consumers: MutableMapping[Topic, ConsumerT] = None
    _callbacks: MutableMapping[Topic, Sequence[Callable]] = None
    _coros: MutableMapping[Topic, CoroCallback] = None

    def __init__(self, name: str = None,
                 topics: Sequence[Topic] = None,
                 callbacks: MutableMapping[Topic, Sequence[Callable]] = None,
                 coros: MutableMapping[Topic, CoroCallback] = None,
                 children: List[StreamT] = None,
                 join_strategy: JoinT = None,
                 app: AppT = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        self.app = app
        self.name = name
        if not isinstance(topics, MutableSequence):
            topics = list(topics)
        self.topics = cast(MutableSequence, topics)
        self._callbacks = callbacks
        self._consumers = OrderedDict()
        self._coros = coros
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
            'callbacks': self._callbacks,
            'coros': self._coros,
            'loop': self.loop,
            'children': self.children,
        }

    def clone(self, **kwargs) -> StreamT:
        return self.__class__(**{**self.info(), **kwargs})

    def combine(self, *nodes: StreamT, **kwargs):
        all_nodes = cast(Tuple[StreamT, ...], (self,)) + nodes
        topics: List[Topic] = []
        callbacks: Dict[Topic, Sequence[Callable]] = {}
        coros: Dict[Topic, CoroCallback] = {}
        for node in all_nodes:
            node = cast(Stream, node)
            topics.extend(node.topics)
            callbacks.update(node._callbacks)
            coros.update(node._coros)
        return self.clone(
            topics=topics,
            callbacks=callbacks,
            coros=coros,
            children=self.children + list(nodes),
        )

    def join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(join.RightJoin(stream=self, fields=fields))

    def left_join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(join.LeftJoin(stream=self, fields=fields))

    def inner_join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(join.InnerJoin(stream=self, fields=fields))

    def outer_join(self, *fields: FieldDescriptorT) -> StreamT:
        return self._join(join.OuterJoin(stream=self, fields=fields))

    def _join(self, join_strategy: JoinT) -> StreamT:
        return self.clone(join_strategy=join_strategy)

    async def on_message(self, topic: Topic, key: K, value: V) -> None:
        callbacks = self._callbacks[topic]
        value = await self.process(key, value)
        if callbacks is not None:
            for callback in callbacks:
                res = callback(value)
                if isinstance(res, Awaitable):
                    value = await res
                else:
                    value = res
        coro = self._coros[topic]
        if coro is not None:
            await coro.send(value, self.on_done)

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
                        callbacks: Sequence[Callable] = None,
                        coro: Callable = None) -> None:
        if topic not in self.topics:
            self.topics.append(topic)
        self._callbacks[topic] = callbacks
        self._coros[topic] = wrap_callback(coro, loop=self.loop)
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
        self._callbacks.pop(topic, None)
        self._coros.pop(topic, None)
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
        for coro in self._coros.values():
            await coro.join()

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
