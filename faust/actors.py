import asyncio
import typing

from collections import defaultdict
from typing import (
    Any, AsyncIterable, AsyncIterator, Awaitable,
    Callable, Iterable, List, MutableMapping,
    MutableSequence, Set, Tuple, Type, Union, cast,
)
from uuid import uuid4
from weakref import WeakSet

from . import Record
from .types import (
    AppT, ChannelT, CodecArg, K, MessageSentCallback, ModelT,
    RecordMetadata, StreamT, TopicT, V,
)
from .types.actors import (
    ActorErrorHandler, ActorFun, ActorInstanceT, ActorRefT, ActorT,
    AsyncIterableActorT, AwaitableActorT, ReplyToArg, SinkT, _T,
)
from .utils.aiter import aenumerate, aiter
from .utils.collections import NodeT
from .utils.futures import maybe_async
from .utils.logging import get_logger
from .utils.objects import cached_property, canoname, qualname
from .utils.services import Service, ServiceProxy

__all__ = [
    'ReqRepRequest',
    'ReqRepResponse',
    'ReplyPromise',
    'ReplyConsumer',
    'ActorInstance',
    'AsyncIterableActor',
    'AwaitableActor',
    'ActorService',
    'Actor',
]

logger = get_logger(__name__)

# --- What is an actor?
#
# An actor is an asynchronous function processing a stream
# of events (messages), that have full programmatic control
# over how that stream is iterated over: right, left, skip.

# Actors are async generators so you can keep
# state between events and associate a result for every event
# by yielding.  Other actors, tasks and coroutines can execute
# concurrently, by suspending the actor when it's waiting for something
# and only resuming when that something is available.
#
# Here's an actor processing a stream of bank withdrawals
# to find transfers larger than $1000, and if it finds one it will send
# an alert:
#
#   class Withdrawal(faust.Record, serializer='json'):
#       account: str
#       amount: float
#
#   app = faust.app('myid', url='kafka://localhost:9092')
#
#   withdrawals_topic = app.topic('withdrawals', value_type=Withdrawal)
#
#   @app.actor(withdrawals_topic)
#   async def alert_on_large_transfer(withdrawal):
#       async for withdrawal in withdrawals:
#           if withdrawal.amount > 1000.0:
#               alert(f'Large withdrawal: {withdrawal}')
#
# The actor above does not ``yield`` so it can never reply to
# an ``ask`` request, or add sinks that further process the results
# of the stream.  This actor is an async generator that yields
# to signal whether it found a large transfer or not:
#
#   @app.actor(withdrawals_topic)
#   async def withdraw(withdrawals):
#       async for withdrawal in withdrawals:
#           if withdrawal.amount > 1000.0:
#               alert(f'Large withdrawal: {withdrawal}')
#               yield True
#           yield False
#
#
# We can add a sink to process the yielded values.  A sink can be
# 1) another actor, 2) a channel/topic, or 3) or callable accepting
# the value as a single argument (that callable can be async or non-async):
#
# async def my_sink(result: bool) -> None:
#    if result:
#        print('Actor withdraw just sent an alert!')
#
# withdraw.add_sink(my_sink)
#
# TIP: Sinks can also be added as an argument to the ``@actor`` decorator:
#      ``@app.actor(sinks


# XXX "namespace" below is the Avro registry key.
# It's going to be included in every single actor request,
# so I figured "org.faust.ReqRepRequest" was too long,
# but maybe that's silly?
class ReqRepRequest(Record, serializer='json', namespace='@RRReq'):
    """Value wrapped in a Request-Reply request."""

    # actor.ask(value) wraps the value in this record
    # so that the receiving actor knows where to send the reply.

    value: ModelT
    reply_to: str
    correlation_id: str


class ReqRepResponse(Record, serializer='json', namespace='@RRRes'):
    """Request-Reply response."""

    key: K
    value: ModelT
    correlation_id: str


class ReplyPromise(asyncio.Future):
    """Reply promise can be awaited to wait until result ready."""
    reply_to: str
    correlation_id: str

    def __init__(self, reply_to: str, correlation_id: str,
                 **kwargs: Any) -> None:
        self.reply_to = reply_to
        self.correlation_id = correlation_id
        super().__init__(**kwargs)

    def fulfill(self, correlation_id: str, value: Any) -> None:
        # If it wasn't for BarrierState we would just use .set_result()
        # directly, but BarrierState.fulfill requires the correlation_id
        # to be sent with it. That way it can mark that part of the map
        # operation as completed.
        assert correlation_id == self.correlation_id
        self.set_result(value)


class BarrierState(ReplyPromise):
    #: This is the size while the messages are being sent.
    #: (it's a tentative total, added to until the total is finalized).
    size: int = 0

    #: This is the actual total when all messages have been sent.
    #: It's set by :meth:`finalize`.
    total: int = 0

    #: The number of results we have received.
    fulfilled: int = 0

    #: Internal queue where results are added to.
    _results: asyncio.Queue

    #: Set of pending replies that this barrier is composed of.
    pending: Set[ReplyPromise]

    def __init__(self, reply_to: str, **kwargs: Any) -> None:
        super().__init__(reply_to=reply_to, correlation_id=None, **kwargs)
        self.pending = set()
        loop: asyncio.AbstractEventLoop = self._loop  # type: ignore
        self._results = asyncio.Queue(maxsize=1000, loop=loop)

    def add(self, p: ReplyPromise) -> None:
        self.pending.add(p)
        self.size += 1

    def finalize(self) -> None:
        self.total = self.size
        # The barrier may have been filled up already at this point,
        if self.fulfilled >= self.total:
            self.set_result(True)

    def fulfill(self, correlation_id: str, value: Any) -> None:
        # ReplyConsumer calls this whenever a new reply is received.
        self._results.put_nowait((correlation_id, value))
        self.fulfilled += 1
        if self.total:
            if self.fulfilled >= self.total:
                self.set_result(True)

    def get_nowait(self) -> Tuple[str, Any]:
        """Return next reply, or raise :exc:`asyncio.QueueEmpty`."""
        return self._results.get_nowait()

    async def iterate(self) -> AsyncIterator[Tuple[str, Any]]:
        """Iterate over results as arrive."""
        get = self._results.get
        get_nowait = self._results.get_nowait
        is_done = self.done
        while not is_done():
            yield await get()
        while 1:
            try:
                yield get_nowait()
            except asyncio.QueueEmpty:
                break


class ReplyConsumer(Service):
    if typing.TYPE_CHECKING:
        _waiting: MutableMapping[str, WeakSet[ReplyPromise]]
    _waiting = None
    _fetchers: MutableMapping[str, asyncio.Future]

    def __init__(self, app: AppT, **kwargs: Any) -> None:
        self.app = app
        self._waiting = defaultdict(WeakSet)
        self._fetchers = {}
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        if self.app.create_reply_topic:
            await self._start_fetcher(self.app.reply_to)

    async def add(self, correlation_id: str, promise: ReplyPromise) -> None:
        reply_topic = promise.reply_to
        if reply_topic not in self._fetchers:
            await self._start_fetcher(reply_topic)
        self._waiting[correlation_id].add(promise)

    async def _start_fetcher(self, topic_name: str) -> None:
        if topic_name not in self._fetchers:
            # set the key as a lock, so it doesn't happen twice
            self._fetchers[topic_name] = None
            # declare the topic
            topic = self._reply_topic(topic_name)
            await topic.maybe_declare()
            await self.sleep(3.0)
            # then create the future
            self._fetchers[topic_name] = self.add_future(
                self._drain_replies(topic))

    async def _drain_replies(self, channel: ChannelT):
        async for reply in channel.stream():
            for promise in self._waiting[reply.correlation_id]:
                promise.fulfill(reply.correlation_id, reply.value)

    def _reply_topic(self, topic: str) -> TopicT:
        return self.app.topic(
            topic,
            partitions=1,
            replicas=0,
            deleting=True,
            retention=self.app.reply_expires,
            value_type=ReqRepResponse,
        )


class ActorInstance(ActorInstanceT, Service):
    logger = logger

    def __init__(self,
                 agent: ActorT,
                 stream: StreamT,
                 it: _T,
                 index: int = None,
                 **kwargs: Any) -> None:
        self.agent = agent
        self.stream = stream
        self.it = it
        self.index = index
        self.actor_task = None
        self.dead = False
        Service.__init__(self, **kwargs)

    async def on_start(self) -> None:
        assert self.actor_task
        self.add_future(self.actor_task)

    async def on_stop(self) -> None:
        self.cancel()

    def cancel(self) -> None:
        if self.actor_task:
            self.actor_task.cancel()

    def __repr__(self) -> str:
        return f'<{self.shortlabel}>'

    @property
    def label(self) -> str:
        return f'Actor*: {self.agent.name}'


class AsyncIterableActor(AsyncIterableActorT, ActorInstance):
    """Used for actor function that yields."""

    def __aiter__(self) -> AsyncIterator:
        return self.it.__aiter__()


class AwaitableActor(AwaitableActorT, ActorInstance):
    """Used for actor function that do not yield."""

    def __await__(self) -> Any:
        return self.it.__await__()


class ActorService(Service):
    # Actors are created at module-scope, and the Service class
    # creates the asyncio loop when created, so we separate the
    # actor service in such a way that we can start it lazily.
    # Actor(ServiceProxy) -> ActorService
    logger = logger

    actor: ActorT
    instances: MutableSequence[ActorRefT]

    def __init__(self, actor: ActorT, **kwargs: Any) -> None:
        self.actor = actor
        self.instances = []
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        # start the actor processor.
        if self.actor.concurrency > 1:
            self.instances[:] = [
                await self._start_one(index)
                for index in range(self.actor.concurrency)
            ]
        else:
            self.instances[:] = [await self._start_one(index=None)]

    async def _start_one(self, index: int = None) -> ActorInstanceT:
        res = await cast(Actor, self.actor)._start_task(index, self.beacon)
        await res.start()
        return res

    @Service.task
    async def _supervisor(self) -> None:
        concurrency = self.actor.concurrency
        instances = self.instances

        # Create an empty list of instances
        instances[:] = [None] * concurrency

        while not self.should_stop:
            # Fill in any missing instances.
            for i in range(concurrency):
                if instances[i] is None:
                    instances[i] = await self._start_one(i)
                elif instances[i].dead and not self.should_stop:
                    self.log.info('Replacing dead actor %r', instances[i])
                    instances[i] = await self._start_one(i)
                else:
                    pass
                await self.sleep(1.0)

    async def on_stop(self) -> None:
        # Actors iterate over infinite streams, and we cannot wait for it
        # to stop.
        # Instead we cancel it and this forces the stream to ack the
        # last message processed (but not the message causing the error
        # to be raised).
        for instance in self.instances:
            await instance.stop()

    @property
    def label(self) -> str:
        return self.actor.label

    @property
    def shortlabel(self) -> str:
        return self.actor.shortlabel


class Actor(ActorT, ServiceProxy):
    _sinks: List[SinkT]

    logger = logger

    def __init__(self, fun: ActorFun,
                 *,
                 name: str = None,
                 app: AppT = None,
                 channel: Union[str, ChannelT] = None,
                 concurrency: int = 1,
                 sink: Iterable[SinkT] = None,
                 on_error: ActorErrorHandler = None) -> None:
        self.app = app
        self.fun: ActorFun = fun
        self.name = name or canoname(self.fun)
        self.channel = self._prepare_channel(channel)
        self.concurrency = concurrency
        self._sinks = list(sink) if sink is not None else []
        self._on_error: ActorErrorHandler = on_error
        ServiceProxy.__init__(self)

    def _prepare_channel(self,
                         channel: Union[str, ChannelT] = None) -> ChannelT:
        channel = self.name if channel is None else channel
        if isinstance(channel, ChannelT):
            return cast(ChannelT, channel)
        elif isinstance(channel, str):
            return self.app.topic(channel)
        raise TypeError(
            f'Channel must be channel, topic, or str; not {type(channel)}')

    def __call__(self, *, index: int = None) -> ActorRefT:
        # The actor function can be reused by other actors/tasks.
        # For example:
        #
        #   @app.actor(logs_topic, through='other-topic')
        #   filter_log_errors_(stream):
        #       async for event in stream:
        #           if event.severity == 'error':
        #               yield event
        #
        #   @app.actor(logs_topic)
        #   def alert_on_log_error(stream):
        #       async for event in filter_log_errors(stream):
        #            alert(f'Error occurred: {event!r}')
        #
        # Calling `res = filter_log_errors(it)` will end you up with
        # an AsyncIterable that you can reuse (but only if the actor
        # function is an `async def` function that yields)
        stream = self.stream(concurrency_index=index)
        res = self.fun(stream)
        typ = cast(Type[ActorInstance], (
            AwaitableActor if isinstance(res, Awaitable)
            else AsyncIterableActor
        ))
        return typ(self, stream, res, index,
                   loop=self.loop, beacon=self.beacon)

    def add_sink(self, sink: SinkT) -> None:
        self._sinks.append(sink)

    def stream(self, **kwargs: Any) -> StreamT:
        s = self.app.stream(self.channel_iterator, loop=self.loop, **kwargs)
        s.add_processor(self._process_reply)
        return s

    def _process_reply(self, event: Any) -> Any:
        if isinstance(event, ReqRepRequest):
            return event.value
        return event

    async def _start_task(self, index: int, beacon: NodeT) -> ActorRefT:
        # If the actor is an async function we simply start it,
        # if it returns an AsyncIterable/AsyncGenerator we start a task
        # that will consume it.
        aref: ActorRefT = self(index=index)
        coro = aref if isinstance(aref, Awaitable) else self._slurp(
            aref, aiter(aref))
        task = asyncio.Task(self._execute_task(coro, aref), loop=self.loop)
        task._beacon = beacon  # type: ignore
        aref.actor_task = task
        return aref

    async def _execute_task(self, coro: Awaitable, aref: ActorRefT) -> None:
        # This executes the actor task itself, and does exception handling.
        try:
            await coro
        except asyncio.CancelledError:
            if aref.stream.current_event:
                await aref.stream.current_event.ack()
            raise
        except Exception as exc:
            self.log.exception('Actor %r raised error: %r', aref, exc)
            if aref.stream.current_event:
                await aref.stream.current_event.ack()
            if self._on_error is not None:
                await self._on_error(self, exc)

            # Mark ActorRef as dead, so that supervisor thread
            # can start a new one.
            aref.dead = True

            raise

    async def _slurp(self, res: ActorRefT, it: AsyncIterator):
        # this is used when the actor returns an AsyncIterator,
        # and simply consumes that async iterator.
        async for value in it:
            self.log.debug('%r yielded: %r', self.fun, value)
            event = res.stream.current_event
            if isinstance(event.value, ReqRepRequest):
                await self._reply(event.key, value, event.value)
            await self._delegate_to_sinks(value)

    async def _delegate_to_sinks(self, value: Any) -> None:
        for sink in self._sinks:
            if isinstance(sink, ActorT):
                await cast(ActorT, sink).send(value=value)
            elif isinstance(sink, ChannelT):
                await cast(TopicT, sink).send(value=value)
            else:
                await maybe_async(cast(Callable, sink)(value))

    async def _reply(self, key: Any, value: Any, req: ReqRepRequest) -> None:
        assert req.reply_to
        await self.app.send(
            req.reply_to,
            key=None,
            value=ReqRepResponse(
                key=key,
                value=value,
                correlation_id=req.correlation_id,
            ),
        )

    async def cast(
            self,
            value: V = None,
            *,
            key: K = None,
            partition: int = None) -> None:
        await self.send(key, value, partition=partition)

    async def ask(
            self,
            value: V = None,
            *,
            key: K = None,
            partition: int = None,
            reply_to: ReplyToArg = None,
            correlation_id: str = None) -> Any:
        p = await self.ask_nowait(
            value,
            key=key,
            partition=partition,
            reply_to=reply_to or self.app.reply_to,
            correlation_id=correlation_id,
            force=True,  # Send immediately, since we are waiting for result.
        )
        await self.app._reply_consumer.add(p.correlation_id, p)  # type: ignore
        await self.app.maybe_start_client()
        return await p

    async def ask_nowait(
            self,
            value: V = None,
            *,
            key: K = None,
            partition: int = None,
            reply_to: ReplyToArg = None,
            correlation_id: str = None,
            force: bool = False) -> ReplyPromise:
        req = self._create_req(key, value, reply_to, correlation_id)
        await self.channel.send(key, req, partition, force=force)
        return ReplyPromise(req.reply_to, req.correlation_id)

    def _create_req(
            self,
            key: K = None,
            value: V = None,
            reply_to: ReplyToArg = None,
            correlation_id: str = None) -> ReqRepRequest:
        topic_name = self._get_strtopic(reply_to)
        correlation_id = correlation_id or str(uuid4())
        return ReqRepRequest(
            value=value,
            reply_to=topic_name,
            correlation_id=correlation_id,
        )

    async def send(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None,
            *,
            reply_to: ReplyToArg = None,
            correlation_id: str = None,
            force: bool = False) -> Awaitable[RecordMetadata]:
        """Send message to topic used by actor."""
        if reply_to:
            value = self._create_req(key, value, reply_to, correlation_id)
        return await self.channel.send(
            key, value, partition,
            key_serializer, value_serializer,
            force=force,
        )

    def _get_strtopic(
            self, topic: Union[str, ChannelT, TopicT, ActorT]) -> str:
        if isinstance(topic, ActorT):
            return self._get_strtopic(cast(ActorT, topic).channel)
        if isinstance(topic, TopicT):
            return cast(TopicT, topic).get_topic_name()
        if isinstance(topic, ChannelT):
            raise ValueError('Channels are unnamed topics')
        return cast(str, topic)

    def send_soon(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            callback: MessageSentCallback = None) -> Awaitable[RecordMetadata]:
        """Send message eventually (non async), to topic used by actor."""
        return self.channel.send_soon(key, value, partition,
                                      key_serializer, value_serializer)

    async def map(
            self,
            values: Union[AsyncIterable, Iterable],
            key: K = None,
            reply_to: ReplyToArg = None) -> AsyncIterator:
        # Map takes only values, but can provide one key that is used for all.
        async for value in self.kvmap(
                ((key, v) async for v in aiter(values)), reply_to):
            yield value

    async def kvmap(
            self,
            items: Union[AsyncIterable[Tuple[K, V]], Iterable[Tuple[K, V]]],
            reply_to: ReplyToArg = None) -> AsyncIterator[str]:
        # kvmap takes (key, value) pairs.
        reply_to = self._get_strtopic(reply_to or self.app.reply_to)

        # BarrierState is the promise that keeps track of pending results.
        # It contains a list of individual ReplyPromises.
        barrier = BarrierState(reply_to)

        async for _ in self._barrier_send(barrier, items, reply_to):
            # Now that we've sent a message, try to see if we have any
            # replies.
            try:
                _, val = barrier.get_nowait()
            except asyncio.QueueEmpty:
                pass
            else:
                yield val
        # All the messages have been sent so finalize the barrier.
        barrier.finalize()

        # Then iterate over the results in the group.
        async for _, value in barrier.iterate():
            yield value

    async def join(
            self,
            values: Union[AsyncIterable[V], Iterable[V]],
            key: K = None,
            reply_to: ReplyToArg = None) -> List[Any]:
        return await self.kvjoin(
            ((key, value) async for value in aiter(values)),
            reply_to=reply_to,
        )

    async def kvjoin(
            self,
            items: Union[AsyncIterable[Tuple[K, V]], Iterable[Tuple[K, V]]],
            reply_to: ReplyToArg = None) -> List[Any]:
        reply_to = self._get_strtopic(reply_to or self.app.reply_to)
        barrier = BarrierState(reply_to)

        # Map correlation_id -> index
        posindex: MutableMapping[str, int] = {
            cid: i
            async for i, cid in aenumerate(
                self._barrier_send(barrier, items, reply_to))
        }

        # All the messages have been sent so finalize the barrier.
        barrier.finalize()

        # wait until all replies received
        await barrier
        # then construct a list in the correct order.
        values: List = [None] * barrier.total
        async for correlation_id, value in barrier.iterate():
            values[posindex[correlation_id]] = value
        return values

    async def _barrier_send(
            self,
            barrier: BarrierState,
            items: Union[AsyncIterable[Tuple[K, V]], Iterable[Tuple[K, V]]],
            reply_to: ReplyToArg) -> AsyncIterator[str]:
        # Send all the values in the group
        # while trying to pop incoming results off.
        async for key, value in aiter(items):
            correlation_id = str(uuid4())
            p = await self.ask_nowait(
                key=key,
                value=value,
                reply_to=reply_to,
                correlation_id=correlation_id)
            # add reply promise to the barrier
            barrier.add(p)

            # the ReplyConsumer will call the barrier whenever a new
            # result comes in.
            await self.app.maybe_start_client()
            await self.app._reply_consumer.add(  # type: ignore
                p.correlation_id, barrier)

            yield correlation_id

    def _repr_info(self) -> str:
        return self.name

    @cached_property
    def channel_iterator(self) -> AsyncIterator:
        # The channel is reused here, so that when ActorService start
        # is called it will start n * concurrency self._start_task() futures
        # that all share the same channel topic.
        return aiter(self.channel)

    @cached_property
    def _service(self) -> ActorService:
        return ActorService(self, beacon=self.app.beacon, loop=self.app.loop)

    @property
    def label(self) -> str:
        return f'{type(self).__name__}: {qualname(self.fun)}'
