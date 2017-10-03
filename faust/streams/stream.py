"""Streams."""
import asyncio
import reprlib
import typing
import weakref

from typing import (
    Any, AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, List,
    Mapping, MutableSequence, Optional, Sequence, Tuple, Union, cast,
)

from mode import Seconds, Service, want_seconds
from mode.utils.types.trees import NodeT

from . import joins
from ._coroutines import CoroCallbackT, wrap_callback

from ..exceptions import ImproperlyConfigured
from ..types import AppT, EventT, K, Message, ModelArg, TopicT
from ..types.joins import JoinT
from ..types.models import FieldDescriptorT
from ..types.streams import (
    GroupByKeyArg, JoinableT, Processor, StreamCoroutine, StreamT,
    T, T_co, T_contra,
)
from ..types.topics import ChannelT
from ..utils.aiolocals import Context, Local
from ..utils.aiter import aenumerate, aiter
from ..utils.futures import StampedeWrapper, maybe_async

__all__ = ['Stream', 'current_event']

__make_flake8_happy_List: List  # XXX flake8 thinks this is unused
__make_flake8_happy_CoroCallbackT: CoroCallbackT
__make_flake8_happy_Message: Message


class _StreamLocal(Local):
    # This holds task-local variables related to streams.

    if typing.TYPE_CHECKING:
        current_event: weakref.ReferenceType[EventT]
    #: Weak reference to the event currently being processed.
    current_event = None


#: Task-local storage (keeps track of e.g. current_event)
_locals = cast(_StreamLocal, Local())


def current_event() -> Optional[EventT]:
    """Returns the event being currently processed, or None."""
    try:
        eventref = getattr(_locals, 'current_event', None)
    except ValueError:  # has no context
        return None
    else:
        return eventref() if eventref is not None else None


async def maybe_forward(value: Any, channel: ChannelT) -> Any:
    if isinstance(value, EventT):
        await value.forward(channel)
    else:
        await channel.send(value=value)
    return value


class Stream(StreamT, Service):

    _processors: MutableSequence[Processor] = None
    _coroutine: CoroCallbackT = None
    _anext_started: bool = False
    _context: Context = None
    _passive = False

    def __init__(self, channel: AsyncIterator[T_co] = None,
                 *,
                 app: AppT = None,
                 processors: Iterable[Processor] = None,
                 coroutine: StreamCoroutine = None,
                 children: List[JoinableT] = None,
                 on_start: Callable = None,
                 join_strategy: JoinT = None,
                 beacon: NodeT = None,
                 concurrency_index: int = None,
                 loop: asyncio.AbstractEventLoop = None) -> None:
        Service.__init__(self, loop=loop, beacon=beacon)
        self.app = app
        self.channel = channel
        self.outbox = self.app.FlowControlQueue(
            maxsize=1, loop=self.loop, clear_on_resume=True)
        self.join_strategy = join_strategy
        self.children = children if children is not None else []
        self.concurrency_index = concurrency_index

        self._processors = list(processors) if processors else []
        if coroutine:
            self._coroutine = wrap_callback(coroutine, None, loop=loop)
            # XXX set coroutine callbacks
            self._coroutine.callback = self._send_to_outbox
        self._on_start = on_start

        # attach beacon to channel, or if iterable attach to current task.
        task = asyncio.Task.current_task(loop=self.loop)
        if task is not None:
            self.task_owner = task

        # Generate message handler
        self._on_stream_event_in = self.app.sensors.on_stream_event_in
        self._on_stream_event_out = self.app.sensors.on_stream_event_out
        self._on_message = self._create_message_handler()

    async def _send_to_outbox(self, value: T_contra) -> None:
        await self.outbox.put(value)

    def add_processor(self, processor: Processor) -> None:
        self._processors.append(processor)

    def info(self) -> Mapping[str, Any]:
        return {
            'app': self.app,
            'channel': self.channel,
            'processors': self._processors,
            'coroutine': self._coroutine,
            'on_start': self._on_start,
            'loop': self.loop,
            'children': self.children,
            'beacon': self.beacon,
        }

    def clone(self, **kwargs: Any) -> Any:
        return self.__class__(**{**self.info(), **kwargs})

    async def items(self) -> AsyncIterator[Tuple[K, T_co]]:
        """Iterate over the stream as ``key, value`` pairs.

        Examples:
            .. sourcecode:: python

                @app.actor(topic)
                async def mytask(stream):
                    async for key, value in stream.items():
                        print(key, value)
        """
        async for event in self.events():
            yield event.key, cast(T_co, event.value)

    async def events(self) -> AsyncIterable[EventT]:
        """Iterate over the stream as events exclusively.

        This means the stream must be iterating over a channel.
        """
        async for _ in self:  # noqa: F841
            if self.current_event is not None:
                yield self.current_event

    async def take(self, max_: int,
                   within: Seconds = None) -> AsyncIterable[Sequence[T_co]]:
        """Buffer n values at a time and yield a list of buffered values.

        Keyword Arguments:
            within: Timeout for when we give up waiting for another value,
                and return the list of values that we have.
                Warning: If this is not set, it can potentially wait forever
                for a new value, and buffered items will not be processed.
        """
        loop = self.loop
        buffer: List[T_co] = []
        add = buffer.append
        within_s = want_seconds(within)

        async def _read_self():
            async for value in self:
                add(value)
                if len(buffer) >= max_:
                    return

        while not self.should_stop:
            # coro #1: Wait for within time
            coro_wait = asyncio.ensure_future(asyncio.sleep(within_s), loop=loop)
            # coro #2: Wait to read upto max_ values from the stream
            coro_read_self = asyncio.ensure_future(_read_self(), loop=loop)

            # wait for first thing to happen: within timeout, or max values
            # read
            done, pending = await asyncio.wait(
                [coro_wait, coro_read_self],
                return_when=asyncio.FIRST_COMPLETED,
                loop=loop)

            if coro_read_self.done():
                # we read enough values from the stream
                coro_wait.cancel()
            else:
                # we timed out
                coro_read_self.cancel()
            yield list(buffer)
            buffer.clear()

    def tee(self, n: int = 2) -> Tuple[StreamT, ...]:
        """Clone stream into n new streams, receiving copies of values.

        This is the stream analog of :func:`itertools.tee`.

        Examples:
            .. sourcecode:: python

                async def processor1(stream):
                    async for value in stream:
                        print(value * 2)

                async def processor2(stream):
                    async for value in stream:
                        print(value / 2)

                @app.actor(topic)
                async def mytask(stream):
                    # duplicate the stream and process it in different ways.
                    a, b = stream.tee(2)
                    await asyncio.gather(processor1(a), processor2(b))
        """
        streams = [
            self.clone(on_start=self.maybe_start)
            for _ in range(n)
        ]

        async def forward(value: T) -> T:
            for stream in streams:
                await stream.send(value)
            return value
        self.add_processor(forward)
        return tuple(streams)

    def enumerate(self,
                  start: int = 0) -> AsyncIterable[Tuple[int, T_co]]:
        """Enumerate values received in this stream.

        Akin to Python's built-in ``enumerate``, but works for an asynchronous
        stream.
        """
        return aenumerate(self, start)

    def through(self, channel: Union[str, ChannelT]) -> StreamT:
        """Forward values to in this stream to channel.

        Send messages received on this stream to another channel,
        and return a new stream that consumes from that channel.

        Notes:
            The messages are forwarded after any processors have been
            applied.

        Example:
            .. sourcecode:: python

                topic = app.topic('foo')

                @app.actor(topic)
                async def mytask(stream):
                    async for value in stream.through(app.topic('bar')):
                        # value was first received in topic 'foo',
                        # then forwarded and consumed from topic 'bar'
                        print(value)
        """
        if self.concurrency_index is not None:
            raise ImproperlyConfigured(
                'Actor with concurrency>1 cannot use stream.through!')
        # ridiculous mypy
        if isinstance(channel, str):
            channelchannel = cast(ChannelT, self.derive_topic(channel))
        else:
            channelchannel = channel

        channel_created = False
        it = aiter(channelchannel)
        through = self.clone(channel=it, on_start=self.maybe_start)

        declare = StampedeWrapper(channelchannel.maybe_declare)

        async def forward(value: T) -> T:
            nonlocal channel_created
            if not channel_created:
                await declare()
                channel_created = True
            event = self.current_event
            return await maybe_forward(event, channelchannel)

        self.add_processor(forward)
        self._enable_passive()
        return through

    def _enable_passive(self) -> None:
        if not self._passive:
            self._passive = True
            self.add_future(self._drainer())

    async def _drainer(self) -> None:
        async for item in self:  # noqa
            ...

    def echo(self, *channels: Union[str, ChannelT]) -> StreamT:
        """Forward values to one or more channels.

        Unlike :meth:`through`, we don't consume from these channels.
        """
        _channels = [
            self.derive_topic(c) if isinstance(c, str) else c
            for c in channels
        ]

        async def echoing(value: T) -> T:
            await asyncio.wait(
                [maybe_forward(value, channel) for channel in _channels],
                loop=self.loop,
                return_when=asyncio.ALL_COMPLETED,
            )
            return value
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

            .. sourcecode:: python

                s = withdrawals_topic.stream()
                # values in this stream are of type Withdrawal
                async for event in s.group_by(Withdrawal.account_id):
                    ...

            Using an async callable to extract a new key:

            .. sourcecode:: python

                s = withdrawals_topic.stream()

                async def get_key(withdrawal):
                    return await aiohttp.get(
                        f'http://e.com/resolve_account/{withdrawal.account_id}'

                async for event in s.group_by(get_key):
                    ...

            Using a regular callable to extract a new key:

            .. sourcecode:: python

                s = withdrawals_topic.stream()

                def get_key(withdrawal):
                    return withdrawal.account_id.upper()

                async for event in s.group_by(get_key):
                    ...
        """
        if self.concurrency_index is not None:
            raise ImproperlyConfigured(
                'Actor with concurrency>1 cannot use stream.group_by!')
        if not name:
            if isinstance(key, FieldDescriptorT):
                name = key.ident
            else:
                raise TypeError(
                    'group_by with callback must set name=topic_suffix')
        if topic is None:
            if not isinstance(self.channel, TopicT):
                raise ValueError('Need to specify topic for non-topic channel')
            suffix = '-' + name + '-repartition'
            topic = cast(TopicT, self.channel).derive(suffix=suffix)
        topic_created = False
        format_key = self._format_key

        grouped = self.clone(channel=aiter(topic), on_start=self.maybe_start)

        declare = StampedeWrapper(topic.maybe_declare)

        async def repartition(value: T) -> T:
            event = self.current_event
            if event is None:
                raise RuntimeError(
                    'Cannot repartition stream with non-topic channel')
            new_key = await format_key(key, value)

            nonlocal topic_created
            if not topic_created:
                await declare()
                topic_created = True
            await event.forward(
                topic.get_topic_name(),
                key=new_key,
            )
            return value
        self.add_processor(repartition)
        self._enable_passive()
        return grouped

    async def _format_key(self, key: GroupByKeyArg, value: T_contra):
        if isinstance(key, FieldDescriptorT):
            return getattr(value, key.field)
        return await maybe_async(key(value))

    def derive_topic(self, name: str,
                     *,
                     key_type: ModelArg = None,
                     value_type: ModelArg = None,
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
            ValueError: if the stream channel is not a topic.
        """
        if isinstance(self.channel, TopicT):
            return cast(TopicT, self.channel).derive(
                topics=[name],
                key_type=key_type,
                value_type=value_type,
                prefix=prefix,
                suffix=suffix,
            )
        raise ValueError('Cannot derive topic from non-topic channel.')

    def combine(self, *nodes: JoinableT, **kwargs: Any) -> StreamT:
        # A combined stream is composed of multiple streams that
        # all share the same outbox.
        # The resulting stream's `on_merge` callback can be used to
        # process values from all the combined streams, and e.g.
        # joins uses this to consolidate multiple values into one.
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

    def _create_message_handler(
            self) -> Callable[[], Awaitable[Tuple[bool, Any]]]:
        # get from channel
        get_next_value = self.channel.__anext__
        # Topic description -> processors
        processors = self._processors
        # Topic description -> special coroutine
        coroutine = self._coroutine
        # Sensor: on_stream_event_in
        on_stream_event_in = self._on_stream_event_in

        # localize this global variable
        threadlocals = _locals
        create_ref = weakref.ref

        async def on_message() -> Tuple[bool, Any]:
            # get message from channel
            value: Any = await get_next_value()

            if isinstance(value, EventT):
                event: EventT = value
                message: Message = event.message
                tp = message.tp
                offset = message.offset

                # call Sensors
                await on_stream_event_in(tp, offset, self, event)

                # set task-local current_event
                threadlocals.current_event = create_ref(event)
                # set Stream._current_event
                self.current_event = event

                value = event.value  # Stream yields Event.value

            # reduce using processors
            for processor in processors:
                value = await maybe_async(processor(value))

            if coroutine is not None:
                # if there is an S-routine we apply that and delegate
                # on done to its callback.
                await coroutine.send(value)
                return True, None
            else:
                return False, value
        return on_message

    async def on_merge(self, value: T = None) -> Optional[T]:
        join_strategy = self.join_strategy
        if join_strategy:
            value = await join_strategy.process(value)
        return value

    async def send(self, value: T_contra) -> None:
        """Send value into stream manually."""
        if isinstance(self.channel, ChannelT):
            await cast(ChannelT, self.channel).put(value)
        else:
            raise NotImplementedError(
                'Cannot send to non-topic channel stream.')

    async def on_start(self) -> None:
        if self._on_start:
            await self._on_start()
        if self._coroutine:
            await self._coroutine.start()

    async def on_stop(self) -> None:
        if self.current_event is not None:
            await self.current_event.ack()
        if self._context is not None:
            self._context.__exit__(None, None, None)

    def __iter__(self) -> Any:
        return self

    def __next__(self) -> Any:
        raise NotImplementedError('Streams are asynchronous: use `async for`')

    def __aiter__(self) -> AsyncIterator:
        self._context = Context(locals=[_locals]).__enter__()
        return self

    async def __anext__(self) -> T:
        # fetch next message and get value from outbox
        value: T = None
        while value is None:  # we iterate until on_merge gives back a value
            if not self._anext_started:
                # setup stuff the first time we are iterated over.
                self._anext_started = True
                await self.maybe_start()
            else:
                # decrement reference count for previous event processed.
                _prev, self.current_event = self.current_event, None
                if _prev is not None:
                    await _prev.ack()
                    _msg = _prev.message
                    await self._on_stream_event_out(
                        _msg.tp, _msg.offset, self, _prev)

            delegated_to_outbox, value = await self._on_message()
            if delegated_to_outbox:
                value = await self.outbox.get()
            value = await self.on_merge(value)
        return value

    def __and__(self, other: Any) -> Any:
        return self.combine(self, other)

    def __copy__(self) -> Any:
        return self.clone()

    def _repr_info(self) -> str:
        if self.children:
            return reprlib.repr(self.children)
        return reprlib.repr(self.channel)

    def _repr_channel(self) -> str:
        return reprlib.repr(self.channel)

    @property
    def label(self) -> str:
        return f'{type(self).__name__}: {self._repr_channel()}'
