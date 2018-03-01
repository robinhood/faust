"""Streams."""
import asyncio
import reprlib
import typing
import weakref
from typing import (
    Any, AsyncIterable, AsyncIterator, Callable, Iterable, List,
    Mapping, MutableSequence, Optional, Sequence, Set, Tuple, Union, cast,
)

from mode import Seconds, Service, want_seconds
from mode.utils.aiter import aenumerate, aiter
from mode.utils.futures import StampedeWrapper, maybe_async
from mode.utils.types.trees import NodeT

from . import joins
from .exceptions import ImproperlyConfigured
from .types import AppT, EventT, K, Message, ModelArg, ModelT, TopicT
from .types.joins import JoinT
from .types.models import FieldDescriptorT
from .types.streams import (
    GroupByKeyArg, JoinableT, Processor, StreamT, T, T_co, T_contra,
)
from .types.topics import ChannelT

__all__ = [
    'Stream',
    'current_event',
]


try:
    from contextvars import ContextVar

    def _inherit_context(*, loop: asyncio.AbstractEventLoop = None) -> None:
        ...
except ImportError:
    from aiocontextvars import ContextVar, Context

    def _inherit_context(*, loop: asyncio.AbstractEventLoop = None) -> None:
        # see module: aiocontextvars.inherit
        # this is the backport of the contextvars module added in CPython 3.7.
        # it provides "thread-locals" for async generators, and asyncio.Task
        # will automatically call this stuff in 3.7, but not in 3.6 so we call
        # this when starting to iterate over the stream (Stream.__aiter__).
        task = asyncio.Task.current_task(loop=loop)
        # note: in actual CPython it's task._context, the aiocontextvars
        # backport is a backport of a previous version of the PEP: :pep:`560`
        task.ctx = Context(Context.current())  # type: ignore


if typing.TYPE_CHECKING:
    _current_event: ContextVar[weakref.ReferenceType[EventT]]
_current_event = ContextVar('current_event')


def current_event() -> Optional[EventT]:
    """Return the event currently being processed, or None."""
    eventref = _current_event.get(None)
    return eventref() if eventref is not None else None


async def maybe_forward(value: Any, channel: ChannelT) -> Any:
    if isinstance(value, EventT):
        await value.forward(channel)
    else:
        await channel.send(value=value)
    return value


class Stream(StreamT[T_co], Service):
    """A stream: async iterator processing events in channels/topics."""

    _processors: MutableSequence[Processor] = None
    _anext_started: bool = False
    _passive = False

    def __init__(self, channel: AsyncIterator[T_co] = None,
                 *,
                 app: AppT = None,
                 processors: Iterable[Processor[T]] = None,
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
            maxsize=self.app.conf.stream_buffer_maxsize,
            loop=self.loop,
            clear_on_resume=True,
        )
        self.join_strategy = join_strategy
        self.children = children if children is not None else []
        self.concurrency_index = concurrency_index

        self._processors = list(processors) if processors else []
        self._on_start = on_start

        # attach beacon to channel, or if iterable attach to current task.
        task = asyncio.Task.current_task(loop=self.loop)
        if task is not None:
            self.task_owner = task

        # Generate message handler
        self._on_stream_event_in = self.app.sensors.on_stream_event_in
        self._on_stream_event_out = self.app.sensors.on_stream_event_out
        self._on_message_out = self.app.sensors.on_message_out

    def get_active_stream(self) -> StreamT:
        """Return the currently active stream.

        A stream can be derived using ``Stream.group_by`` etc,
        so if this stream was used to create another derived
        stream, this function will return the stream being actively
        consumed from.  E.g. in the example::

            >>> @app.agent()
            ... async def agent(a):
            ..      a = a
            ...     b = a.group_by(Withdrawal.account_id)
            ...     c = b.through('backup_topic')
            ...     async for value in c:
            ...         ...

        The return value of ``a.get_active_stream()`` would be ``c``.

        Notes:
            The chain of streams that leads to the active stream
            is decided by the :attr:`link` attribute, and getting
            the active stream is just traversing this linked-list::

                >>> def get_active_stream(self):
                ...     node = self
                ...     while node.link:
                ...         node = node.link
        """
        node = self
        seen: Set[StreamT] = set()
        while node.link:
            if node in seen:
                raise RuntimeError(
                    'Loop in Stream.link linked-list. Call support!')
            seen.add(node)
            node = node.link
        return node

    async def _send_to_outbox(self, value: T_contra) -> None:
        await self.outbox.put(value)

    def add_processor(self, processor: Processor[T]) -> None:
        """Add processor callback executed whenever a new event is received.

        Processor functions can be async or non-async, must accept
        a single argument, and should return the value, mutated or not.

        For example a processor handling a stream of numbers may modify
        the value::

            def double(value: int) -> int:
                return value * 2

            stream.add_processor(double)
        """
        self._processors.append(processor)

    def info(self) -> Mapping[str, Any]:
        """Return stream settings as a dictionary."""
        # used by e.g. .clone to reconstruct keyword arguments
        # needed to create a clone of the stream.
        return {
            'app': self.app,
            'channel': self.channel,
            'processors': self._processors,
            'on_start': self._on_start,
            'loop': self.loop,
            'children': self.children,
            'beacon': self.beacon,
            'concurrency_index': self.concurrency_index,
        }

    def clone(self, **kwargs: Any) -> Any:
        """Create a clone of this stream.

        Notes:
            If the cloned stream is supposed to "supercede" this stream,
            you should set `stream.link = cloned_stream` so that
            :meth:`get_active_stream` returns the cloned stream.
        """
        return self.__class__(**{**self.info(), **kwargs})

    async def items(self) -> AsyncIterator[Tuple[K, T_co]]:
        """Iterate over the stream as ``key, value`` pairs.

        Examples:
            .. sourcecode:: python

                @app.agent(topic)
                async def mytask(stream):
                    async for key, value in stream.items():
                        print(key, value)
        """
        async for event in self.events():
            yield event.key, cast(T_co, event.value)

    async def events(self) -> AsyncIterable[EventT]:
        """Iterate over the stream as events exclusively.

        This means the stream must be iterating over a channel,
        or at least an iterable of event objects.
        """
        async for _ in self:  # noqa: F841
            if self.current_event is not None:
                yield self.current_event

    async def take(self, max_: int,
                   within: Seconds) -> AsyncIterable[Sequence[T_co]]:
        """Buffer n values at a time and yield a list of buffered values.

        Arguments:
            within: Timeout for when we give up waiting for another value,
                and process the values we have.
                Warning: If there's no timeout (i.e. `timeout=None`),
                the agent is likely to stall and block buffered events for an
                unreasonable length of time(!).
        """
        buffer: List[T_co] = []
        add = buffer.append
        timeout = want_seconds(within) if within else None

        async def _buffer() -> None:
            async for value in self:
                add(value)
                if len(buffer) >= max_:
                    break

        while not self.should_stop:
            if not await self.wait_for_stopped(_buffer(), timeout=timeout):
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

                @app.agent(topic)
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
        """Enumerate values received on this stream.

        Unlike Python's built-in ``enumerate``, this works with
        async generators.
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

                @app.agent(topic)
                async def mytask(stream):
                    async for value in stream.through(app.topic('bar')):
                        # value was first received in topic 'foo',
                        # then forwarded and consumed from topic 'bar'
                        print(value)
        """
        if self.concurrency_index is not None:
            raise ImproperlyConfigured(
                'Agent with concurrency>1 cannot use stream.through!')
        # ridiculous mypy
        if isinstance(channel, str):
            channelchannel = cast(ChannelT, self.derive_topic(channel))
        else:
            channelchannel = channel

        channel_created = False
        channel_it = aiter(channelchannel)
        if self.link is not None:
            raise ImproperlyConfigured(
                'Stream is already using group_by/through')
        self.link = through = self.clone(
            channel=channel_it,
            on_start=self.maybe_start,
        )

        declare = StampedeWrapper(channelchannel.maybe_declare)

        async def forward(value: T) -> T:
            nonlocal channel_created
            if not channel_created:
                await declare()
                channel_created = True
            event = self.current_event
            return await maybe_forward(event, channelchannel)

        self.add_processor(forward)
        self._enable_passive(cast(ChannelT, channel_it))
        return through

    def _enable_passive(self, channel: ChannelT) -> None:
        if not self._passive:
            self._passive = True
            self.add_future(self._passive_drainer(channel))

    async def _passive_drainer(self, channel: ChannelT) -> None:
        try:
            async for item in self:  # noqa
                ...
        except BaseException as exc:
            # forward the exception to the final destination channel,
            # e.g. in through/group_by/etc.
            await channel.throw(exc)

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
                 topic: TopicT = None,
                 partitions: int = None) -> StreamT:
        """Create new stream that repartitions the stream using a new key.

        Arguments:
            key: The key argument decides how the new key is generated,
                it can be a field descriptor, a callable, or an async
                callable.

                Note: The ``name`` argument must be provided if the key
                    argument is a callable.

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
        channel: ChannelT
        if self.concurrency_index is not None:
            raise ImproperlyConfigured(
                'Agent with concurrency>1 cannot use stream.group_by!')
        if not name:
            if isinstance(key, FieldDescriptorT):
                name = cast(FieldDescriptorT, key).ident
            else:
                raise TypeError(
                    'group_by with callback must set name=topic_suffix')
        if topic is not None:
            channel = topic
        else:
            suffix = '-' + self.app.conf.id + '-' + name + '-repartition'
            p = partitions if partitions else self.app.conf.topic_partitions
            channel = cast(ChannelT, self.channel).derive(
                suffix=suffix, partitions=p, internal=True)
        topic_created = False
        format_key = self._format_key

        channel_it = aiter(channel)
        if self.link is not None:
            raise ImproperlyConfigured('Stream already uses group_by/through')
        self.link = grouped = self.clone(
            channel=channel_it,
            on_start=self.maybe_start,
        )
        declare = StampedeWrapper(channel.maybe_declare)

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
            await event.forward(channel, key=new_key)
            return value
        self.add_processor(repartition)
        self._enable_passive(cast(ChannelT, channel_it))
        return grouped

    async def _format_key(self, key: GroupByKeyArg, value: T_contra) -> str:
        if isinstance(key, FieldDescriptorT):
            return cast(FieldDescriptorT, key).getattr(cast(ModelT, value))
        return await maybe_async(cast(Callable, key)(value))

    def derive_topic(self, name: str,
                     *,
                     key_type: ModelArg = None,
                     value_type: ModelArg = None,
                     prefix: str = '',
                     suffix: str = '') -> TopicT:
        """Create Topic description derived from the K/V type of this stream.

        Arguments:
            name: Topic name.

            key_type: Specific key type to use for this topic.
                If not set, the key type of this stream will be used.
            value_type: Specific value type to use for this topic.
                If not set, the value type of this stream will be used.

        Raises:
            ValueError: if the stream channel is not a topic.
        """
        if isinstance(self.channel, TopicT):
            return self.channel.derive_topic(
                topics=[name],
                key_type=key_type,
                value_type=value_type,
                prefix=prefix,
                suffix=suffix,
            )
        raise ValueError('Cannot derive topic from non-topic channel.')

    async def throw(self, exc: BaseException) -> None:
        await cast(ChannelT, self.channel).throw(exc)

    def combine(self, *nodes: JoinableT, **kwargs: Any) -> StreamT:
        # A combined stream is composed of multiple streams that
        # all share the same outbox.
        # The resulting stream's `on_merge` callback can be used to
        # process values from all the combined streams, and e.g.
        # joins uses this to consolidate multiple values into one.
        self.link = stream = self.clone(
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

    async def on_merge(self, value: T = None) -> Optional[T]:
        # TODO for joining streams
        # The join strategy.process method can return None
        # to eat the value, and on the next event create a merged
        # event out of the previous event and new event.
        join_strategy = self.join_strategy
        if join_strategy:
            value = await join_strategy.process(value)
        return value

    async def send(self, value: T_contra) -> None:
        """Send value into stream locally (bypasses topic)."""
        if isinstance(self.channel, ChannelT):
            await cast(ChannelT, self.channel).put(value)
        else:
            raise NotImplementedError(
                'Cannot send to non-topic channel stream.')

    async def on_start(self) -> None:
        if self._on_start:
            await self._on_start()

    async def on_stop(self) -> None:
        if self.current_event is not None:
            await self.ack(self.current_event)

    def __iter__(self) -> Any:
        return self

    def __next__(self) -> Any:
        raise NotImplementedError('Streams are asynchronous: use `async for`')

    async def __aiter__(self) -> AsyncIterator:
        _inherit_context(loop=self.loop)
        await self.maybe_start()
        on_merge = self.on_merge
        on_stream_event_out = self._on_stream_event_out
        on_message_out = self._on_message_out

        # get from channel
        get_next_value = self.channel.__anext__
        # Topic description -> processors
        processors = self._processors
        # Sensor: on_stream_event_in
        on_stream_event_in = self._on_stream_event_in

        # localize global variables
        create_ref = weakref.ref
        _maybe_async = maybe_async
        event_cls = EventT
        _current_event_contextvar = _current_event

        while not self.should_stop:
            # wait for next message
            value: Any = None
            while value is None:  # we iterate until on_merge gives value.
                # decrement reference count for previous event processed.
                _prev, self.current_event = self.current_event, None
                if _prev is not None:
                    # This inlines self.ack
                    last_stream_to_ack = _prev.ack()
                    prev_message = _prev.message
                    ptp = prev_message.tp
                    poffset = prev_message.offset
                    await on_stream_event_out(ptp, poffset, self, _prev)
                    if last_stream_to_ack:
                        await on_message_out(ptp, poffset, prev_message)

                # get message from channel
                channel_value: Any = await get_next_value()

                if isinstance(channel_value, event_cls):
                    event: EventT = channel_value
                    message: Message = event.message
                    tp = message.tp
                    offset = message.offset

                    # call Sensors
                    await on_stream_event_in(tp, offset, self, event)

                    # set task-local current_event
                    _current_event_contextvar.set(create_ref(event))
                    # set Stream._current_event
                    self.current_event = event

                    # Stream yields Event.value
                    value = event.value
                else:
                    value = channel_value
                    self.current_event = None

                # reduce using processors
                for processor in processors:
                    value = await _maybe_async(processor(value))
                value = await on_merge(value)
            yield value

    async def __anext__(self) -> T:
        ...

    async def ack(self, event: EventT) -> bool:
        """Ack event.

        This will decrease the reference count of the event message by one,
        and when the reference count reaches zero, the worker will
        commit the offset so that the message will not be seen by a worker
        again.

        Arguments:
            event: Event to ack.
        """
        # WARNING: This function is duplicated in __aiter__
        last_stream_to_ack = event.ack()
        message = event.message
        tp = message.tp
        offset = message.offset
        await self._on_stream_event_out(tp, offset, self, event)
        if last_stream_to_ack:
            await self._on_message_out(tp, offset, message)
        return last_stream_to_ack

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
        # used as textual description in graphs
        return f'{type(self).__name__}: {self._repr_channel()}'