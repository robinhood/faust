"""Streams."""
import asyncio
import reprlib
import typing
import weakref

from typing import (
    Any, AsyncIterator, Awaitable, Callable, List,
    Mapping, MutableSequence, Optional, Sequence, Tuple, Type, Union, cast,
)

from ..types import K, TopicT, Message
from ..types.joins import JoinT
from ..types.models import Event, FieldDescriptorT
from ..types.streams import (
    GroupByKeyArg, JoinableT, Processor, StreamCoroutine, StreamT,
)
from ..types.topics import TopicConsumerT
from ..utils.aiolocals import Context, Local
from ..utils.aiter import aenumerate
from ..utils.futures import maybe_async
from ..utils.logging import get_logger
from ..utils.services import Service
from ..utils.types.collections import NodeT

from ._coroutines import CoroCallbackT, wrap_callback
from . import _constants
from . import joins

__all__ = ['Stream', 'current_event']

__make_flake8_happy_List: List  # XXX flake8 thinks this is unused
__make_flake8_happy_CoroCallbackT: CoroCallbackT
__make_flake8_happy_Message: Message

logger = get_logger(__name__)


class _StreamLocal(Local):
    # This holds task-local variables related to streams.

    if typing.TYPE_CHECKING:
        current_event: weakref.ReferenceType[Event]
    #: Weak reference to the event currently being processed.
    current_event = None


#: Task-local storage (keeps track of e.g. current_event)
_locals = cast(_StreamLocal, Local())


def current_event() -> Optional[Event]:
    """Returns the event being currently processed, or None."""
    eventref = getattr(_locals, 'current_event', None)
    return eventref() if eventref is not None else None


class Stream(StreamT, JoinableT, Service):

    _processors: MutableSequence[Processor] = None
    _coroutine: CoroCallbackT = None
    _anext_started: bool = False
    _current_event: Event = None
    _context: Context = None

    def __init__(self,
                 *,
                 name: str = None,
                 source: AsyncIterator = None,
                 processors: Sequence[Processor] = None,
                 coroutine: StreamCoroutine = None,
                 children: List[JoinableT] = None,
                 on_start: Callable = None,
                 join_strategy: JoinT = None,
                 beacon: NodeT = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        Service.__init__(self, loop=loop, beacon=None)
        self.name = name
        self.source = source
        self._processors = list(processors) if processors else []
        if coroutine:
            self._coroutine = wrap_callback(coroutine, None, loop=loop)
            # XXX set coroutine callbacks
            self._coroutine.callback = self._send_to_outbox
        self._on_start = on_start
        self.join_strategy = join_strategy
        self.children = children if children is not None else []
        self.outbox = asyncio.Queue(maxsize=1, loop=self.loop)

        # attach beacon to source, or if iterable attach to current task.
        task = asyncio.Task.current_task(loop=self.loop)
        if task is not None:
            self.task_owner = task
        if source is not None and hasattr(source, 'beacon'):
            self.beacon = self.source.beacon.new(self)  # type: ignore
        elif task is not None and hasattr(task, '_beacon'):
            self.beacon = task._beacon.new(self)  # type: ignore

        # Generate message handler
        self._on_message = None
        self._on_stream_event_in = None
        self._on_stream_event_out = None
        if self.source:
            try:
                app = self.source.app  # type: ignore
                self._on_stream_event_in = app.sensors.on_stream_event_in
                self._on_stream_event_out = app.sensors.on_stream_event_out
            except AttributeError:
                pass
            self._on_message = self._create_message_handler()

    async def _send_to_outbox(self, event: Event) -> None:
        await self.outbox.put(event)

    def add_processor(self, processor: Processor) -> None:
        self._processors.append(processor)

    async def items(self) -> AsyncIterator[Tuple[K, Event]]:
        """Iterate over the stream as ``key, event`` pairs.

        Examples:
            .. code-block:: python

                @app.actor(topic)
                async def mytask(events):
                    async for key, event in events.items():
                        print(key, event)
        """
        async for event in self:
            yield event.req.key, event

    async def take(self, max_events: int,
                   within: float = None) -> AsyncIterator[Sequence[Event]]:
        """Buffer n events at a time and yield lists of events.

        Keyword Arguments:
            within: Timeout for when we give up waiting for another event,
                and return the list of events that we have.  If this is not
                set, it can potentially wait forever.
        """
        buffer: List[Event] = []
        add = buffer.append
        wait_for = asyncio.wait_for
        if within:
            while 1:
                try:
                    add(await wait_for(self.__anext__(), timeout=within))
                except asyncio.TimeoutError:
                    yield list(buffer)
                    buffer.clear()
                else:
                    if len(buffer) >= max_events:
                        yield list(buffer)
                        buffer.clear()
        else:
            async for event in self:
                add(event)
                if len(buffer) >= max_events:
                    yield list(buffer)
                    buffer.clear()

    def tee(self, n: int = 2) -> Tuple[StreamT, ...]:
        """Clone stream into n new streams, receiving copies of events.

        This is the stream analog of :func:`itertools.tee`.

        Examples:
            .. code-block:: python

                async def processor1(stream):
                    async for item in stream:
                        print(item.value * 2)

                async def processor2(stream):
                    async for item in stream:
                        print(item.value / 2)

                @app.actor(topic)
                async def mytask(events):
                    # duplicate the stream and process it in different ways.
                    a, b = events.tee(2)
                    await asyncio.gather(
                        processor1(a),
                        processor2(b))
        """
        streams = [
            self.clone(on_start=self.maybe_start)
            for _ in range(n)
        ]

        async def forward(event: Event) -> Event:
            for stream in streams:
                await stream.send(event)
            return event
        self.add_processor(forward)
        return tuple(streams)

    def enumerate(self,
                  start: int = 0) -> AsyncIterator[Tuple[int, Event]]:
        """Enumerate events received in this stream.

        Akin to Python's built-in ``enumerate``, but works for an asynchronous
        stream.
        """
        return aenumerate(self, start)

    def through(self, topic: Union[str, TopicT]) -> StreamT:
        """Forward events to new topic and consume from that topic.

        Send messages received on this stream to another topic,
        and return a new stream that consumes from that topic.

        Notes:
            The messages are forwarded after any processors have been
            applied.

        Example:
            .. code-block:: python

                topic = app.topic('foo')

                @app.actor(topic)
                async def mytask(events):
                    async for event in events.through('bar'):
                        # event was first received in topic 'foo',
                        # then forwarded and consumed from topic 'bar'
                        print(event)
        """
        if isinstance(topic, str):
            topic = self.derive_topic(topic)
        topic = topic

        async def forward(event: Event) -> Event:
            await event.forward(topic)
            return event
        self.add_processor(forward)
        return self.clone(source=topic, on_start=self.maybe_start)

    def echo(self, *topics: Union[str, TopicT]) -> StreamT:
        """Forward events to one or more topics.

        Unlike :meth:`through`, we don't consume from these topics.
        """
        _topics = [
            self.derive_topic(t) if isinstance(t, str) else t
            for t in topics
        ]

        async def echoing(event: Event) -> Event:
            for t in _topics:
                await event.forward(t)
            return event
        self.add_processor(echoing)
        return self

    def group_by(self, key: GroupByKeyArg,
                 *,
                 name: str = None,
                 topic: TopicT = None) -> StreamT:
        """Create new stream that repartitions the stream using a new key.

        Arguments:
            key: The key argument decides how the new key is generated,
                it can be a field descriptor, a callable, or an async
                callable.

                Note: The ``name`` argument must be provided if the key
                    argument is a callable.

        Keyword Arguments:
            name: Suffix to use for repartitioned topics.
                This argument is required if `key` is a callable.

        Examples:
            Using a field descriptor to use a field in the event as the new
            key:

            .. code-block:: python

                s = withdrawals_topic.stream()
                async for event in s.group_by(Withdrawal.account_id):
                    ...

            Using an async callable to extract a new key:

            .. code-block:: python

                s = withdrawals_topic.stream()

                async def get_key(withdrawal):
                    return await aiohttp.get(
                        'http://example.com/resolve_account/{}'.format(
                            withdrawal.account_id))

                async for event in s.group_by(get_key):
                    ...

            Using a regular callable to extract a new key:

            .. code-block:: python

                s = withdrawals_topic.stream()

                def get_key(withdrawal):
                    return withdrawal.account_id.upper()

                async for event in s.group_by(get_key):
                    ...
        """
        if not name:
            if isinstance(key, FieldDescriptorT):
                name = key.ident
            else:
                raise TypeError(
                    'group_by with callback must set name=topic_suffix')
        if topic is None:
            if not isinstance(self.source, TopicConsumerT):
                raise ValueError('Need to specify topic for non-topic source')
            suffix = '-' + name + _constants.REPARTITION_TOPIC_SUFFIX
            source = cast(TopicConsumerT, self.source)
            topic = source.topic.derive(suffix=suffix)
        format_key = self._format_key

        async def repartition(event: Event) -> Event:
            new_key = await format_key(key, event)
            await event.forward(
                event.req.message.topic + suffix,
                key=new_key,
            )
            return event
        self.add_processor(repartition)
        return self.clone(source=topic, on_start=self.maybe_start)

    async def _format_key(self, key: GroupByKeyArg, event: Event):
        if isinstance(key, FieldDescriptorT):
            return getattr(event, key.field)
        return await maybe_async(key(event))

    def derive_topic(self, name: str,
                     *,
                     key_type: Type = None,
                     value_type: Type = None,
                     prefix: str = '',
                     suffix: str = '') -> TopicT:
        """Create topic derived from the key/value type of this stream.

        Arguments:
            name: Topic name.

        Keyword Arguments:
            key_type: Specific key type to use for this topic.
                If not set, the key type of this stream will be used.
            value_type: Specific value type to use for this topic.
                If not set, the value type of this stream will be used.

        Raises:
            TypeError: if the types used by topics in this stream
                is not uniform.
        """
        if isinstance(self.source, TopicConsumerT):
            return cast(TopicConsumerT, self.source).topic.derive(
                topics=[name],
                key_type=key_type,
                value_type=value_type,
                prefix=prefix,
                suffix=suffix,
            )
        raise ValueError('Cannot derive topic from non-topic source.')

    def asdict(self) -> Mapping[str, Any]:
        return {
            'name': self.name,
            'source': self.source,
            'processors': self._processors,
            'coroutine': self._coroutine,
            'on_start': self._on_start,
            'loop': self.loop,
            'children': self.children,
            'beacon': self.beacon,
        }

    def combine(self, *nodes: JoinableT, **kwargs: Any) -> StreamT:
        # A combined stream is composed of multiple streams that
        # all share the same outbox.
        # The resulting stream's `on_merge` callback can be used to
        # process events from all the combined streams, and e.g.
        # joins uses this to consolidate multiple events into one.
        stream = self.clone(
            children=self.children + list(nodes),
        )
        for node in stream.children:
            node.outbox = stream.outbox
        return stream

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

    def _create_message_handler(self) -> Callable[[], Awaitable[None]]:
        # get from source
        get_message = self.source.__anext__
        # Topic description -> processors
        processors = self._processors
        # Topic description -> special coroutine
        coroutine = self._coroutine
        # Sensor: on_stream_event_in
        on_stream_event_in = self._on_stream_event_in

        # localize this global variable
        locals = _locals
        create_ref = weakref.ref

        async def on_message() -> None:
            # get message from source
            event: Event = await get_message()
            message: Message = event.req.message

            # call Sensors
            await on_stream_event_in(message.tp, message.offset, self, event)

            # set task-local current_event
            locals.current_event = create_ref(event)

            # reduce using processors
            for processor in processors:
                event = await maybe_async(processor(event))

            if coroutine is not None:
                # if there is an S-routine we apply that and delegate
                # on done to its callback.
                await coroutine.send(event)
            else:
                # otherwise we send directly to outbox
                await self._send_to_outbox(event)
        return on_message

    async def on_merge(self, value: Event = None) -> Optional[Event]:
        join_strategy = self.join_strategy
        if join_strategy:
            value = await join_strategy.process(value)
        return value

    async def send(self, value: Event) -> None:
        """Send event into stream manually."""
        if isinstance(self.source, TopicConsumerT):
            await cast(TopicConsumerT, self.source).put(value)
        else:
            raise NotImplementedError(
                'Cannot send to non-topic source stream.')

    async def on_start(self) -> None:
        if self._on_start:
            await self._on_start()
        if self._coroutine:
            await self._coroutine.start()

    async def on_stop(self) -> None:
        if self._current_event is not None:
            self._current_event.ack()
        if self._context is not None:
            self._context.__exit__()

    async def on_aiter_start(self) -> None:
        """Callback called when this stream is first iterated over."""
        ...

    def __iter__(self) -> Any:
        return self

    def __next__(self) -> Event:
        raise NotImplementedError('Streams are asynchronous: use __aiter__')

    def __aiter__(self) -> AsyncIterator:
        self._context = Context(locals=[_locals]).__enter__()
        return self

    async def __anext__(self) -> Event:
        if not self._anext_started:
            # setup stuff the first time we are iterated over.
            self._anext_started = True
            await self.maybe_start()
            await self.on_aiter_start()
        else:
            # decrement reference count for previous event processed.
            _prev, self._current_event = self._current_event, None
            if _prev is not None:
                _prev.ack()
            _msg = _prev.req.message
            on_stream_event_out = self._on_stream_event_out
            if on_stream_event_out is not None:
                await on_stream_event_out(_msg.tp, _msg.offset, self, _prev)

        # fetch next message and get value from outbox
        event: Event = None
        while not event:  # we iterate until on_merge gives back an event
            await self._on_message()
            event = await self.on_merge(cast(Event, await self.outbox.get()))
        self._current_event = event
        return event

    def __and__(self, other: JoinableT) -> StreamT:
        return self.combine(self, other)

    def clone(self, **kwargs: Any) -> Any:
        return self.__class__(**{**self.asdict(), **kwargs})

    def __copy__(self) -> Any:
        return self.clone()

    def _repr_info(self) -> str:
        if self.children:
            return reprlib.repr(self.children)
        return reprlib.repr(self.source)

    @property
    def label(self) -> str:
        return '{}: {}'.format(type(self).__name__, self.source)
