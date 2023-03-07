"""Streams."""
import asyncio
import os
import reprlib
import typing
import weakref

from asyncio import CancelledError
from contextvars import ContextVar
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Callable,
    Dict,
    Iterable,
    Iterator,
    List,
    Mapping,
    MutableSequence,
    NamedTuple,
    Optional,
    Sequence,
    Set,
    Tuple,
    Union,
    cast,
)

from mode import Seconds, Service, get_logger, shortlabel, want_seconds
from mode.utils.aiter import aenumerate, aiter
from mode.utils.futures import current_task, maybe_async, notify
from mode.utils.queues import ThrowableQueue
from mode.utils.typing import Deque
from mode.utils.types.trees import NodeT

from . import joins
from .exceptions import ImproperlyConfigured, Skip
from .types import AppT, ConsumerT, EventT, K, ModelArg, ModelT, TP, TopicT
from .types.joins import JoinT
from .types.models import FieldDescriptorT
from .types.serializers import SchemaT
from .types.streams import (
    GroupByKeyArg,
    JoinableT,
    Processor,
    StreamT,
    T,
    T_co,
    T_contra,
)
from .types.topics import ChannelT
from .types.tuples import Message

NO_CYTHON = bool(os.environ.get('NO_CYTHON', False))

if not NO_CYTHON:  # pragma: no cover
    try:
        from ._cython.streams import StreamIterator as _CStreamIterator
    except ImportError:
        _CStreamIterator = None
else:  # pragma: no cover
    _CStreamIterator = None

__all__ = [
    'Stream',
    'current_event',
]

logger = get_logger(__name__)

if typing.TYPE_CHECKING:  # pragma: no cover
    _current_event: ContextVar[Optional[weakref.ReferenceType[EventT]]]
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


class _LinkedListDirection(NamedTuple):
    attr: str
    getter: Callable[[StreamT], Optional[StreamT]]


_LinkedListDirectionFwd = _LinkedListDirection('_next', lambda n: n._next)
_LinkedListDirectionBwd = _LinkedListDirection('_prev', lambda n: n._prev)


class Stream(StreamT[T_co], Service):
    """A stream: async iterator processing events in channels/topics."""

    logger = logger
    # Service starting/stopping logs use severity DEBUG in this class.
    mundane_level = 'debug'

    #: Number of events processed by this instance so far.
    events_total: int = 0

    _processors: MutableSequence[Processor]
    _anext_started = False
    _passive = False
    _finalized = False
    _passive_started: asyncio.Event

    def __init__(self,
                 channel: AsyncIterator[T_co],
                 *,
                 app: AppT,
                 processors: Iterable[Processor[T]] = None,
                 combined: List[JoinableT] = None,
                 on_start: Callable = None,
                 join_strategy: JoinT = None,
                 beacon: NodeT = None,
                 concurrency_index: int = None,
                 prev: StreamT = None,
                 active_partitions: Set[TP] = None,
                 enable_acks: bool = True,
                 prefix: str = '',
                 loop: asyncio.AbstractEventLoop = None) -> None:
        Service.__init__(self, loop=loop, beacon=beacon)
        self.app = app
        self.channel = channel
        self.outbox = self.app.FlowControlQueue(
            maxsize=self.app.conf.stream_buffer_maxsize,
            loop=self.loop,
            clear_on_resume=True,
        )
        self._passive_started = asyncio.Event(loop=self.loop)
        self.join_strategy = join_strategy
        self.combined = combined if combined is not None else []
        self.concurrency_index = concurrency_index
        self._prev = prev
        self.active_partitions = active_partitions
        self.enable_acks = enable_acks
        self.prefix = prefix

        self._processors = list(processors) if processors else []
        self._on_start = on_start

        # attach beacon to channel, or if iterable attach to current task.
        task = current_task(loop=self.loop)
        if task is not None:
            self.task_owner = task

        # Generate message handler
        self._on_stream_event_in = self.app.sensors.on_stream_event_in
        self._on_stream_event_out = self.app.sensors.on_stream_event_out
        self._on_message_in = self.app.sensors.on_message_in
        self._on_message_out = self.app.sensors.on_message_out

        self._skipped_value = object()

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
            is decided by the :attr:`_next` attribute. To get
            to the active stream we just traverse this linked-list::

                >>> def get_active_stream(self):
                ...     node = self
                ...     while node._next:
                ...         node = node._next
        """
        return list(self._iter_ll_forwards())[-1]

    def get_root_stream(self) -> StreamT:
        """Get the root stream that this stream was derived from."""
        return list(self._iter_ll_backwards())[-1]

    def _iter_ll_forwards(self) -> Iterator[StreamT]:
        return self._iter_ll(_LinkedListDirectionFwd)

    def _iter_ll_backwards(self) -> Iterator[StreamT]:
        return self._iter_ll(_LinkedListDirectionBwd)

    def _iter_ll(self, dir_: _LinkedListDirection) -> Iterator[StreamT]:
        node: Optional[StreamT] = self
        seen: Set[StreamT] = set()
        while node:
            if node in seen:
                raise RuntimeError(
                    'Loop in Stream.{dir_.attr}: Call support!')
            seen.add(node)
            yield node
            node = dir_.getter(node)

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
            'combined': self.combined,
            'beacon': self.beacon,
            'concurrency_index': self.concurrency_index,
            'prev': self._prev,
            'active_partitions': self.active_partitions,
        }

    def clone(self, **kwargs: Any) -> StreamT:
        """Create a clone of this stream.

        Notes:
            If the cloned stream is supposed to supersede this stream,
            like in ``group_by``/``through``/etc., you should use
            :meth:`_chain` instead so `stream._next = cloned_stream`
            is set and :meth:`get_active_stream` returns the cloned stream.
        """
        return self.__class__(**{**self.info(), **kwargs})

    def _chain(self, **kwargs: Any) -> StreamT:
        assert not self._finalized
        self._next = new_stream = self.clone(
            on_start=self.maybe_start,
            prev=self,
            # move processors to active stream
            processors=list(self._processors),
            **kwargs,
        )
        # delete moved processors from self
        self._processors.clear()
        return new_stream

    def noack(self) -> 'StreamT':
        """Create new stream where acks are manual."""
        self._next = new_stream = self.clone(
            enable_acks=False,
        )
        return new_stream

    async def items(self) -> AsyncIterator[Tuple[K, T_co]]:
        """Iterate over the stream as ``key, value`` pairs.

        Examples:
            .. sourcecode:: python

                @app.agent(topic)
                async def mytask(stream):
                    async for key, value in stream.items():
                        print(key, value)
        """
        async for value in self:
            if self.current_event is not None:
                # yield processed value from iterator, not from event
                yield self.current_event.key, value

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
            max_: Max number of messages to receive. When more than this
                number of messages are received within the specified number of
                seconds then we flush the buffer immediately.
            within: Timeout for when we give up waiting for another value,
                and process the values we have.
                Warning: If there's no timeout (i.e. `timeout=None`),
                the agent is likely to stall and block buffered events for an
                unreasonable length of time(!).
        """
        buffer: List[T_co] = []
        events: List[EventT] = []
        buffer_add = buffer.append
        event_add = events.append
        buffer_size = buffer.__len__
        buffer_full = asyncio.Event(loop=self.loop)
        buffer_consumed = asyncio.Event(loop=self.loop)
        timeout = want_seconds(within) if within else None
        stream_enable_acks: bool = self.enable_acks

        buffer_consuming: Optional[asyncio.Future] = None

        channel_it = aiter(self.channel)

        # We add this processor to populate the buffer, and the stream
        # is passively consumed in the background (enable_passive below).
        async def add_to_buffer(value: T) -> T:
            try:
                # buffer_consuming is set when consuming buffer after timeout.
                nonlocal buffer_consuming
                if buffer_consuming is not None:
                    try:
                        await buffer_consuming
                    finally:
                        buffer_consuming = None
                buffer_add(cast(T_co, value))
                event = self.current_event
                if event is None:
                    raise RuntimeError(
                        'Take buffer found current_event is None')
                event_add(event)
                if buffer_size() >= max_:
                    # signal that the buffer is full and should be emptied.
                    buffer_full.set()
                    # strict wait for buffer to be consumed after buffer full.
                    # If max is 1000, we are not allowed to return 1001 values.
                    buffer_consumed.clear()
                    await self.wait(buffer_consumed)
            except CancelledError:  # pragma: no cover
                raise
            except Exception as exc:
                self.log.exception('Error adding to take buffer: %r', exc)
                await self.crash(exc)
            return value

        # Disable acks to ensure this method acks manually
        # events only after they are consumed by the user
        self.enable_acks = False

        self.add_processor(add_to_buffer)
        self._enable_passive(cast(ChannelT, channel_it))
        try:
            while not self.should_stop:
                # wait until buffer full, or timeout
                await self.wait_for_stopped(buffer_full, timeout=timeout)
                if buffer:
                    # make sure background thread does not add new items to
                    # buffer while we read.
                    buffer_consuming = self.loop.create_future()
                    try:
                        yield list(buffer)
                    finally:
                        buffer.clear()
                        for event in events:
                            await self.ack(event)
                        events.clear()
                        # allow writing to buffer again
                        notify(buffer_consuming)
                        buffer_full.clear()
                        buffer_consumed.set()
                else:  # pragma: no cover
                    pass
            else:  # pragma: no cover
                pass

        finally:
            # Restore last behaviour of "enable_acks"
            self.enable_acks = stream_enable_acks
            self._processors.remove(add_to_buffer)

    def enumerate(self, start: int = 0) -> AsyncIterable[Tuple[int, T_co]]:
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
        if self._finalized:
            # if agent restart we reuse the same stream object
            # which already have done the stream.through()
            # so on iteration we set the finalized flag
            # and make this through() a noop.
            return self
        if self.concurrency_index is not None:
            raise ImproperlyConfigured(
                'Agent with concurrency>1 cannot use stream.through!')
        # ridiculous mypy
        if isinstance(channel, str):
            channelchannel = cast(ChannelT, self.derive_topic(channel))
        else:
            channelchannel = channel

        channel_it = aiter(channelchannel)
        if self._next is not None:
            raise ImproperlyConfigured(
                'Stream is already using group_by/through')
        through = self._chain(channel=channel_it)

        async def forward(value: T) -> T:
            event = self.current_event
            return await maybe_forward(event, channelchannel)

        self.add_processor(forward)
        self._enable_passive(cast(ChannelT, channel_it), declare=True)
        return through

    def _enable_passive(self, channel: ChannelT, *,
                        declare: bool = False) -> None:
        if not self._passive:
            self._passive = True
            self.add_future(self._passive_drainer(channel, declare))

    async def _passive_drainer(self, channel: ChannelT,
                               declare: bool = False) -> None:
        try:
            if declare:
                await channel.maybe_declare()
            self._passive_started.set()
            try:
                async for item in self:  # pragma: no cover
                    ...
            except BaseException as exc:
                # forward the exception to the final destination channel,
                # e.g. in through/group_by/etc.
                await channel.throw(exc)
        finally:
            self._channel_stop_iteration(channel)
            self._passive = False

    def _channel_stop_iteration(self, channel: Any) -> None:
        try:
            on_stop_iteration = channel.on_stop_iteration
        except AttributeError:
            pass
        else:
            on_stop_iteration()

    def echo(self, *channels: Union[str, ChannelT]) -> StreamT:
        """Forward values to one or more channels.

        Unlike :meth:`through`, we don't consume from these channels.
        """
        _channels = [
            self.derive_topic(c) if isinstance(c, str) else c for c in channels
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

    def group_by(self,
                 key: GroupByKeyArg,
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
                        f'http://e.com/resolve_account/{withdrawal.account_id}')

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
        if self._finalized:
            # see note in self.through()
            return self
        channel: ChannelT
        if self.concurrency_index is not None:
            raise ImproperlyConfigured(
                'Agent with concurrency>1 cannot use stream.group_by!')
        if not name:
            if isinstance(key, FieldDescriptorT):
                name = key.ident
            else:
                raise TypeError(
                    'group_by with callback must set name=topic_suffix')
        if topic is not None:
            channel = topic
        else:

            prefix = ''
            if self.prefix and not cast(TopicT, self.channel).has_prefix:
                prefix = self.prefix + '-'
            suffix = f'-{name}-repartition'
            p = partitions if partitions else self.app.conf.topic_partitions
            channel = cast(ChannelT, self.channel).derive(
                prefix=prefix, suffix=suffix, partitions=p, internal=True)
        format_key = self._format_key

        channel_it = aiter(channel)
        if self._next is not None:
            raise ImproperlyConfigured('Stream already uses group_by/through')
        grouped = self._chain(channel=channel_it)

        async def repartition(value: T) -> T:
            event = self.current_event
            if event is None:
                raise RuntimeError(
                    'Cannot repartition stream with non-topic channel')
            new_key = await format_key(key, value)
            await event.forward(channel, key=new_key)
            return value

        self.add_processor(repartition)
        self._enable_passive(cast(ChannelT, channel_it), declare=True)
        return grouped

    def filter(self, fun: Processor[T]) -> StreamT:
        """Filter values from stream using callback.

        The callback may be a traditional function, lambda function,
        or an `async def` function.

        This method is useful for filtering events before repartitioning
        a stream.

        Examples:
            >>> async for v in stream.filter(lambda: v > 1000).group_by(...):
            ...     # do something
        """
        async def on_value(value: T) -> T:
            if not await maybe_async(fun(value)):
                raise Skip()
            else:
                return value

        self.add_processor(on_value)

        return self

    async def _format_key(self, key: GroupByKeyArg, value: T_contra) -> str:
        try:
            if isinstance(key, FieldDescriptorT):
                return key.getattr(cast(ModelT, value))
            return await maybe_async(cast(Callable, key)(value))
        except BaseException as exc:
            self.log.exception('Error in grouping key : %r', exc)
            raise Skip() from exc

    def derive_topic(self,
                     name: str,
                     *,
                     schema: SchemaT = None,
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
            return cast(TopicT, self.channel).derive_topic(
                topics=[name],
                schema=schema,
                key_type=key_type,
                value_type=value_type,
                prefix=prefix,
                suffix=suffix,
            )
        raise ValueError('Cannot derive topic from non-topic channel.')

    async def throw(self, exc: BaseException) -> None:
        """Send exception to stream iteration."""
        await cast(ChannelT, self.channel).throw(exc)

    def combine(self, *nodes: JoinableT, **kwargs: Any) -> StreamT:
        """Combine streams and tables into joined stream."""
        # A combined stream is composed of multiple streams that
        # all share the same outbox.
        # The resulting stream's `on_merge` callback can be used to
        # process values from all the combined streams, and e.g.
        # joins uses this to consolidate multiple values into one.
        if self._finalized:
            # see note in self.through()
            return self
        stream = self._chain(combined=self.combined + list(nodes))
        for node in stream.combined:
            node.contribute_to_stream(stream)
        return stream

    def contribute_to_stream(self, active: StreamT) -> None:
        """Add stream as node in joined stream."""
        self.outbox = active.outbox

    async def remove_from_stream(self, stream: StreamT) -> None:
        """Remove as node in a joined stream."""
        await self.stop()

    def join(self, *fields: FieldDescriptorT) -> StreamT:
        """Create stream where events are joined."""
        return self._join(joins.RightJoin(stream=self, fields=fields))

    def left_join(self, *fields: FieldDescriptorT) -> StreamT:
        """Create stream where events are joined by LEFT JOIN."""
        return self._join(joins.LeftJoin(stream=self, fields=fields))

    def inner_join(self, *fields: FieldDescriptorT) -> StreamT:
        """Create stream where events are joined by INNER JOIN."""
        return self._join(joins.InnerJoin(stream=self, fields=fields))

    def outer_join(self, *fields: FieldDescriptorT) -> StreamT:
        """Create stream where events are joined by OUTER JOIN."""
        return self._join(joins.OuterJoin(stream=self, fields=fields))

    def _join(self, join_strategy: JoinT) -> StreamT:
        return self.clone(join_strategy=join_strategy)

    async def on_merge(self, value: T = None) -> Optional[T]:
        """Signal called when an event is to be joined."""
        # TODO for joining streams
        # The join strategy.process method can return None
        # to eat the value, and on the next event create a merged
        # event out of the previous event and new event.
        join_strategy = self.join_strategy
        if join_strategy:
            value = await join_strategy.process(value)
        return value

    async def on_start(self) -> None:
        """Signal called when the stream starts."""
        if self._on_start:
            await self._on_start()
        if self._passive:
            await self._passive_started.wait()

    async def stop(self) -> None:
        """Stop this stream."""
        # Stop all related streams (created by .through/.group_by/etc.)
        for s in cast(Stream, self.get_root_stream())._iter_ll_forwards():
            await Service.stop(cast(Service, s))

    async def on_stop(self) -> None:
        """Signal that the stream is stopping."""
        self._passive = False
        self._passive_started.clear()
        for table_or_stream in self.combined:
            await table_or_stream.remove_from_stream(self)

    def __iter__(self) -> Any:
        return self

    def __next__(self) -> T:
        raise NotImplementedError('Streams are asynchronous: use `async for`')

    def __aiter__(self) -> AsyncIterator[T_co]:  # pragma: no cover
        if _CStreamIterator is not None:
            return self._c_aiter()
        else:
            return self._py_aiter()

    async def _c_aiter(self) -> AsyncIterator[T_co]:  # pragma: no cover
        self.log.dev('Using Cython optimized __aiter__')
        skipped_value = self._skipped_value
        self._finalized = True
        started_by_aiter = await self.maybe_start()
        it = _CStreamIterator(self)
        try:
            while not self.should_stop:
                do_ack = self.enable_acks
                value, sensor_state = await it.next()  # noqa: B305
                try:
                    if value is not skipped_value:
                        self.events_total += 1
                        yield value
                finally:
                    event, self.current_event = self.current_event, None
                    it.after(event, do_ack, sensor_state)
        except StopAsyncIteration:
            # We are not allowed to propagate StopAsyncIteration in __aiter__
            # (if we do, it'll be converted to RuntimeError by CPython).
            # It can be raised when streaming over a list:
            #    async for value in app.stream([1, 2, 3, 4]):
            #       ...
            # To support that, we just return here and that will stop
            # the iteration.
            return
        finally:
            self._channel_stop_iteration(self.channel)
            if started_by_aiter:
                await self.stop()
                self.service_reset()

    def _set_current_event(self, event: EventT = None) -> None:
        if event is None:
            _current_event.set(None)
        else:
            _current_event.set(weakref.ref(event))
        self.current_event = event

    async def _py_aiter(self) -> AsyncIterator[T_co]:
        self._finalized = True
        loop = self.loop
        started_by_aiter = await self.maybe_start()
        on_merge = self.on_merge
        on_stream_event_out = self._on_stream_event_out
        on_message_out = self._on_message_out

        # get from channel
        channel = self.channel
        if isinstance(channel, ChannelT):
            chan_is_channel = True
            chan = cast(ChannelT, self.channel)
            chan_queue = chan.queue
            chan_queue_empty = chan_queue.empty
            chan_errors = chan_queue._errors
            chan_quick_get = chan_queue.get_nowait
        else:
            chan_is_channel = False
            chan_queue = cast(ThrowableQueue, None)
            chan_queue_empty = cast(Callable, None)
            chan_errors = cast(Deque, None)
            chan_quick_get = cast(Callable, None)
        chan_slow_get = channel.__anext__
        # Topic description -> processors
        processors = self._processors
        # Sensor: on_stream_event_in
        on_stream_event_in = self._on_stream_event_in

        # localize global variables
        create_ref = weakref.ref
        _maybe_async = maybe_async
        event_cls = EventT
        _current_event_contextvar = _current_event

        consumer: ConsumerT = self.app.consumer
        unacked: Set[Message] = consumer.unacked
        add_unacked: Callable[[Message], None] = unacked.add
        acking_topics: Set[str] = self.app.topics.acking_topics
        on_message_in = self._on_message_in
        sleep = asyncio.sleep
        trace = self.app.trace
        _shortlabel = shortlabel
        sensor_state: Optional[Dict] = None
        skipped_value = self._skipped_value

        try:
            while not self.should_stop:
                event = None
                do_ack = self.enable_acks  # set to False to not ack event.
                # wait for next message
                value: Any = None
                # we iterate until on_merge gives value.
                while value is None and event is None:
                    await sleep(0, loop=loop)
                    # get message from channel
                    # This inlines ThrowableQueue.get for performance:
                    # We selectively call `await Q.put`/`Q.put_nowait`,
                    # and prefer the latter if the queue is non-empty.
                    channel_value: Any
                    if chan_is_channel:
                        if chan_errors:
                            raise chan_errors.popleft()
                        if chan_queue_empty():
                            channel_value = await chan_slow_get()
                        else:
                            channel_value = chan_quick_get()
                    else:
                        # chan is an AsyncIterable
                        channel_value = await chan_slow_get()

                    if isinstance(channel_value, event_cls):
                        event = channel_value
                        message = event.message
                        topic = message.topic
                        tp = message.tp
                        offset = message.offset

                        if topic in acking_topics and not message.tracked:
                            message.tracked = True
                            # This inlines Consumer.track_message(message)
                            add_unacked(message)
                            on_message_in(message.tp, message.offset, message)
                            # XXX ugh this should be in the consumer somehow

                        # call Sensors
                        sensor_state = on_stream_event_in(
                            tp, offset, self, event)

                        # set task-local current_event
                        _current_event_contextvar.set(create_ref(event))
                        # set Stream._current_event
                        self.current_event = event

                        # Stream yields Event.value
                        value = event.value
                    else:
                        value = channel_value
                        self.current_event = None
                        sensor_state = None

                    # reduce using processors
                    try:
                        for processor in processors:
                            with trace(f'processor-{_shortlabel(processor)}'):
                                value = await _maybe_async(processor(value))
                        value = await on_merge(value)
                    except Skip:
                        value = skipped_value

                if value is skipped_value:
                    continue
                self.events_total += 1
                try:
                    yield value
                finally:
                    self.current_event = None
                    if do_ack and event is not None:
                        # This inlines self.ack
                        last_stream_to_ack = event.ack()
                        message = event.message
                        tp = event.message.tp
                        offset = event.message.offset
                        on_stream_event_out(
                            tp, offset, self, event, sensor_state)
                        if last_stream_to_ack:
                            on_message_out(tp, offset, message)
        except StopAsyncIteration:
            # We are not allowed to propagate StopAsyncIteration in __aiter__
            # (if we do, it'll be converted to RuntimeError by CPython).
            # It can be raised when streaming over a list:
            #    async for value in app.stream([1, 2, 3, 4]):
            #       ...
            # To support that, we just return here and that will stop
            # the iteration.
            return
        finally:
            self._channel_stop_iteration(channel)
            if started_by_aiter:
                # if `async for` called Stream.start()
                # we need to stop it before ending iteration.
                await self.stop()
                # reset to allow calling .start again on next `async for`
                self.service_reset()

    async def __anext__(self) -> T:  # pragma: no cover
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
        self._on_stream_event_out(tp, offset, self, event)
        if last_stream_to_ack:
            self._on_message_out(tp, offset, message)
        return last_stream_to_ack

    def __and__(self, other: Any) -> Any:
        return self.combine(self, other)

    def __copy__(self) -> Any:
        return self.clone()

    def _repr_info(self) -> str:
        if self.combined:
            return reprlib.repr(self.combined)
        return reprlib.repr(self.channel)

    @property
    def label(self) -> str:
        """Return description of stream, used in graphs and logs."""
        return f'{type(self).__name__}: {self._repr_channel()}'

    def _repr_channel(self) -> str:
        return reprlib.repr(self.channel)

    @property
    def shortlabel(self) -> str:
        """Return short description of stream."""
        # used for shortlabel(stream), which is used by statsd to generate ids
        # note: str(channel) returns topic name when it's a topic, so
        # this will be:
        #    "Channel: <ANON>", for channel or
        #    "Topic: withdrawals", for a topic.
        # statsd then uses that as part of the id.
        return f'Stream: {self._human_channel()}'

    def _human_channel(self) -> str:
        if self.combined:
            return '&'.join(s._human_channel() for s in self.combined)
        return f'{type(self.channel).__name__}: {self.channel}'
