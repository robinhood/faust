"""Agent implementation."""
import asyncio
import typing
from time import time
from typing import (
    Any,
    AsyncIterable,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    MutableSequence,
    MutableSet,
    Optional,
    Set,
    Tuple,
    Type,
    Union,
    cast,
)
from uuid import uuid4
from weakref import WeakSet, WeakValueDictionary

from mode import (
    CrashingSupervisor,
    Service,
    ServiceT,
    SupervisorStrategyT,
)
from mode.proxy import ServiceProxy
from mode.utils.aiter import aenumerate, aiter
from mode.utils.futures import maybe_async
from mode.utils.objects import cached_property, canoname, qualname
from mode.utils.text import shorten_fqdn
from mode.utils.types.trees import NodeT

from faust.exceptions import ImproperlyConfigured

from faust.types import (
    AppT,
    ChannelT,
    CodecArg,
    EventT,
    K,
    Message,
    MessageSentCallback,
    ModelArg,
    RecordMetadata,
    StreamT,
    TP,
    TopicT,
    V,
)
from faust.types.agents import (
    ActorRefT,
    ActorT,
    AgentErrorHandler,
    AgentFun,
    AgentT,
    AgentTestWrapperT,
    ReplyToArg,
    SinkT,
)

from .actor import Actor, AsyncIterableActor, AwaitableActor
from .models import ReqRepRequest, ReqRepResponse
from .replies import BarrierState, ReplyPromise

if typing.TYPE_CHECKING:  # pragma: no cover
    from faust.app.base import App
else:
    class App: ...   # noqa

__all__ = ['Agent']

# --- What is an agent?
#
# An agent is an asynchronous function processing a stream
# of events (messages), that have full programmatic control
# over how that stream is iterated over: right, left, skip.

# Agents are async generators so you can keep
# state between events and associate a result for every event
# by yielding.  Other agents, tasks and coroutines can execute
# concurrently, by suspending the agent when it's waiting for something
# and only resuming when that something is available.
#
# Here's an agent processing a stream of bank withdrawals
# to find transfers larger than $1000, and if it finds one it will send
# an alert:
#
#   class Withdrawal(faust.Record, serializer='json'):
#       account: str
#       amount: float
#
#   app = faust.app('myid', broker='kafka://localhost:9092')
#
#   withdrawals_topic = app.topic('withdrawals', value_type=Withdrawal)
#
#   @app.agent(withdrawals_topic)
#   async def alert_on_large_transfer(withdrawal):
#       async for withdrawal in withdrawals:
#           if withdrawal.amount > 1000.0:
#               alert(f'Large withdrawal: {withdrawal}')
#
# The agent above does not ``yield`` so it can never reply to
# an ``ask`` request, or add sinks that further process the results
# of the stream.  This agent is an async generator that yields
# to signal whether it found a large transfer or not:
#
#   @app.agent(withdrawals_topic)
#   async def withdraw(withdrawals):
#       async for withdrawal in withdrawals:
#           if withdrawal.amount > 1000.0:
#               alert(f'Large withdrawal: {withdrawal}')
#               yield True
#           yield False
#
# We can add a sink to process the yielded values.  A sink can be
# 1) another agent, 2) a channel/topic, or 3) or callable accepting
# the value as a single argument (that callable can be async or non-async):
#
# async def my_sink(result: bool) -> None:
#    if result:
#        print('Agent withdraw just sent an alert!')
#
# withdraw.add_sink(my_sink)
#
# TIP: Sinks can also be added as an argument to the ``@agent`` decorator:
#      ``@app.agent(sinks=[other_agent])``.


class AgentService(Service):
    # Agents are created at module-scope, and the Service class
    # creates the asyncio loop when created, so we separate the
    # agent service in such a way that we can start it lazily.
    # Agent(ServiceProxy) -> AgentService.

    agent: AgentT
    instances: MutableSequence[ActorRefT]
    supervisor: SupervisorStrategyT = None

    def __init__(self, agent: AgentT, **kwargs: Any) -> None:
        self.agent = agent
        self.supervisor = None
        super().__init__(**kwargs)

    async def _start_one(self,
                         index: Optional[int] = None,
                         active_partitions: Optional[Set[TP]] = None,
                         stream: StreamT = None) -> ActorT:
        # an index of None means there's only one instance,
        # and `index is None` is used as a test by functions that
        # disallows concurrency.
        index = index if self.agent.concurrency > 1 else None
        return await cast(Agent, self.agent)._start_task(
            index, active_partitions, stream, self.beacon)

    async def _start_one_supervised(
            self,
            index: Optional[int] = None,
            active_partitions: Optional[Set[TP]] = None,
            stream: StreamT = None) -> ActorT:
        aref = await self._start_one(index, active_partitions, stream)
        self.supervisor.add(aref)
        await aref.maybe_start()
        return aref

    async def _start_for_partitions(self,
                                    active_partitions: Set[TP]) -> ActorT:
        assert active_partitions
        self.log.info('Starting actor for partitions %s', active_partitions)
        return await self._start_one_supervised(None, active_partitions)

    async def on_start(self) -> None:
        self.supervisor = self._new_supervisor()
        await self._on_start_supervisor()

    def _new_supervisor(self) -> SupervisorStrategyT:
        return self._get_supervisor_strategy()(
            max_restarts=100.0,
            over=1.0,
            replacement=self._replace_actor,
            loop=self.loop,
            beacon=self.beacon,
        )

    async def _replace_actor(self, service: ServiceT, index: int) -> ServiceT:
        aref = cast(ActorRefT, service)
        return await self._start_one(
            index, aref.active_partitions, aref.stream)

    def _get_supervisor_strategy(self) -> Type[SupervisorStrategyT]:
        SupervisorStrategy = self.agent.supervisor_strategy
        if SupervisorStrategy is None:
            SupervisorStrategy = self.agent.app.conf.agent_supervisor
        return SupervisorStrategy

    async def _on_start_supervisor(self) -> None:
        active_partitions = self._get_active_partitions()
        for i in range(self.agent.concurrency):
            res = await self._start_one(i, active_partitions)
            self.supervisor.add(res)
        await self.supervisor.start()

    def _get_active_partitions(self) -> Optional[Set[TP]]:
        active_partitions: Optional[Set[TP]] = None
        if self.agent.isolated_partitions:
            # when we start our first agent, we create the set of
            # partitions early, and save it in ._pending_active_partitions.
            # That way we can update the set once partitions are assigned,
            # and the actor we started may be assigned one of the partitions.
            active_partitions = self.agent._pending_active_partitions = set()
        return active_partitions

    async def on_stop(self) -> None:
        # Agents iterate over infinite streams, so we cannot wait for it
        # to stop.
        # Instead we cancel it and this forces the stream to ack the
        # last message processed (but not the message causing the error
        # to be raised).
        await self._stop_supervisor()

    async def _stop_supervisor(self) -> None:
        if self.supervisor:
            await self.supervisor.stop()
            self.supervisor = None

    @property
    def label(self) -> str:
        return self.agent.label

    @property
    def shortlabel(self) -> str:
        return self.agent.shortlabel


class Agent(AgentT, ServiceProxy):
    """Agent.

    This is the type of object returned by the ``@app.agent`` decorator.
    """

    # channel is loaded lazily on .channel property access
    # to make sure configuration is not accessed when agent created
    # at module-scope.
    _channel: Optional[ChannelT] = None
    _channel_arg: Optional[Union[str, ChannelT]]
    _channel_kwargs: Dict[str, Any]
    _channel_iterator: Optional[AsyncIterator] = None
    _sinks: List[SinkT]

    _actors: MutableSet[ActorRefT]
    _actor_by_partition: MutableMapping[TP, ActorRefT]

    #: This mutable set is used by the first agent we start,
    #: so that we can update its active_partitions later
    #: (in on_partitions_assigned, when we know what partitions we get).
    _pending_active_partitions: Optional[Set[TP]] = None

    _first_assignment_done: bool = False

    def __init__(self,
                 fun: AgentFun,
                 *,
                 app: AppT,
                 name: str = None,
                 channel: Union[str, ChannelT] = None,
                 concurrency: int = 1,
                 sink: Iterable[SinkT] = None,
                 on_error: AgentErrorHandler = None,
                 supervisor_strategy: Type[SupervisorStrategyT] = None,
                 help: str = None,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 isolated_partitions: bool = False,
                 **kwargs: Any) -> None:
        self.app = app
        self.fun: AgentFun = fun
        self.name = name or canoname(self.fun)
        # key-type/value_type arguments only apply when a channel
        # is not set
        if key_type is not None:
            assert channel is None or isinstance(channel, str)
        self._key_type = key_type
        if value_type is not None:
            assert channel is None or isinstance(channel, str)
        self._value_type = value_type
        self._channel_arg = channel
        self._channel_kwargs = kwargs
        self.concurrency = concurrency or 1
        self.isolated_partitions = isolated_partitions
        self.help = help or ''
        self._sinks = list(sink) if sink is not None else []
        self._on_error: Optional[AgentErrorHandler] = on_error
        self.supervisor_strategy = supervisor_strategy
        self._actors = WeakSet()
        self._actor_by_partition = WeakValueDictionary()
        if self.isolated_partitions and self.concurrency > 1:
            raise ImproperlyConfigured(
                'Agent concurrency must be 1 when using isolated partitions')
        ServiceProxy.__init__(self)

    def cancel(self) -> None:
        for actor in self._actors:
            actor.cancel()

    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        if self.isolated_partitions:
            return await self.on_isolated_partitions_revoked(revoked)
        return await self.on_shared_partitions_revoked(revoked)

    async def on_partitions_assigned(self, assigned: Set[TP]) -> None:
        if self.isolated_partitions:
            return await self.on_isolated_partitions_assigned(assigned)
        return await self.on_shared_partitions_assigned(assigned)

    async def on_isolated_partitions_revoked(self, revoked: Set[TP]) -> None:
        self.log.dev('Partitions revoked')
        for tp in revoked:
            aref: Optional[ActorRefT] = self._actor_by_partition.pop(tp, None)
            if aref is not None:
                await aref.on_isolated_partition_revoked(tp)

    async def on_isolated_partitions_assigned(self, assigned: Set[TP]) -> None:
        for tp in sorted(assigned):
            await self._assign_isolated_partition(tp)

    async def _assign_isolated_partition(self, tp: TP) -> None:
        if (not self._first_assignment_done and
                not self._actor_by_partition):
            self._first_assignment_done = True
            # if this is the first time we are assigned
            # we need to reassign the agent we started at boot to
            # one of the partitions.
            self._on_first_isolated_partition_assigned(tp)
        await self._maybe_start_isolated(tp)

    def _on_first_isolated_partition_assigned(self, tp: TP) -> None:
        assert self._actors
        assert len(self._actors) == 1
        self._actor_by_partition[tp] = next(iter(self._actors))
        if self._pending_active_partitions is not None:
            assert not self._pending_active_partitions
            self._pending_active_partitions.add(tp)

    async def _maybe_start_isolated(self, tp: TP) -> None:
        try:
            aref = self._actor_by_partition[tp]
        except KeyError:
            aref = await self._start_isolated(tp)
            self._actor_by_partition[tp] = aref
        await aref.on_isolated_partition_assigned(tp)

    async def _start_isolated(self, tp: TP) -> ActorT:
        return await self._service._start_for_partitions({tp})

    async def on_shared_partitions_revoked(self, revoked: Set[TP]) -> None:
        ...

    async def on_shared_partitions_assigned(self, assigned: Set[TP]) -> None:
        ...

    def info(self) -> Mapping:
        return {
            'app': self.app,
            'fun': self.fun,
            'name': self.name,
            'channel': self.channel,
            'concurrency': self.concurrency,
            'help': self.help,
            'sinks': self._sinks,
            'on_error': self._on_error,
            'supervisor_strategy': self.supervisor_strategy,
            'isolated_partitions': self.isolated_partitions,
        }

    def clone(self, *, cls: Type[AgentT] = None, **kwargs: Any) -> AgentT:
        return (cls or type(self))(**{**self.info(), **kwargs})

    def test_context(self,
                     channel: ChannelT = None,
                     supervisor_strategy: SupervisorStrategyT = None,
                     **kwargs: Any) -> AgentTestWrapperT:  # pragma: no cover
        # flow control into channel queues are disabled at startup,
        # so need to resume that.
        self.app.flow_control.resume()
        return self.clone(
            cls=AgentTestWrapper,
            channel=channel if channel is not None else self.app.channel(),
            supervisor_strategy=supervisor_strategy or CrashingSupervisor,
            original_channel=self.channel,
            **kwargs)

    def _prepare_channel(self,
                         channel: Union[str, ChannelT] = None,
                         internal: bool = True,
                         key_type: ModelArg = None,
                         value_type: ModelArg = None,
                         **kwargs: Any) -> ChannelT:
        app = self.app
        channel = f'{app.conf.id}-{self.name}' if channel is None else channel
        if isinstance(channel, ChannelT):
            return cast(ChannelT, channel)
        elif isinstance(channel, str):
            return app.topic(
                channel,
                internal=internal,
                key_type=key_type,
                value_type=value_type,
                **kwargs)
        raise TypeError(
            f'Channel must be channel, topic, or str; not {type(channel)}')

    def __call__(self, *,
                 index: int = None,
                 active_partitions: Set[TP] = None,
                 stream: StreamT = None) -> ActorRefT:
        # The agent function can be reused by other agents/tasks.
        # For example:
        #
        #   @app.agent(logs_topic, through='other-topic')
        #   filter_log_errors_(stream):
        #       async for event in stream:
        #           if event.severity == 'error':
        #               yield event
        #
        #   @app.agent(logs_topic)
        #   def alert_on_log_error(stream):
        #       async for event in filter_log_errors(stream):
        #            alert(f'Error occurred: {event!r}')
        #
        # Calling `res = filter_log_errors(it)` will end you up with
        # an AsyncIterable that you can reuse (but only if the agent
        # function is an `async def` function that yields)
        if stream is None:
            stream = self.stream(
                concurrency_index=index,
                active_partitions=active_partitions,
            )
        else:
            # reusing actor stream after agent restart
            assert stream.concurrency_index == index
            assert stream.active_partitions == active_partitions
        return self.actor_from_stream(stream)

    def actor_from_stream(self, stream: StreamT) -> ActorRefT:
        res = self.fun(stream)
        typ = cast(Type[Actor],
                   (AwaitableActor
                    if isinstance(res, Awaitable) else AsyncIterableActor))
        return typ(
            self,
            stream,
            res,
            index=stream.concurrency_index,
            active_partitions=stream.active_partitions,
            loop=self.loop,
            beacon=self.beacon,
        )

    def add_sink(self, sink: SinkT) -> None:
        if sink not in self._sinks:
            self._sinks.append(sink)

    def stream(self, active_partitions: Set[TP] = None,
               **kwargs: Any) -> StreamT:
        channel = self.channel_iterator
        channel = cast(TopicT, channel).clone(
            is_iterator=False,
            active_partitions=active_partitions,
        )
        if active_partitions is not None:
            assert channel.active_partitions == active_partitions
        s = self.app.stream(
            channel,
            loop=self.loop,
            active_partitions=active_partitions,
            **kwargs)
        s.add_processor(self._maybe_unwrap_reply_request)
        return s

    def _maybe_unwrap_reply_request(self, value: V) -> Any:
        if isinstance(value, ReqRepRequest):
            return value.value
        return value

    async def _start_task(self,
                          index: Optional[int],
                          active_partitions: Optional[Set[TP]] = None,
                          stream: StreamT = None,
                          beacon: NodeT = None) -> ActorRefT:
        # If the agent is an async function we simply start it,
        # if it returns an AsyncIterable/AsyncGenerator we start a task
        # that will consume it.
        actor = self(
            index=index,
            active_partitions=active_partitions,
            stream=stream,
        )
        return await self._prepare_actor(actor, beacon)

    async def _prepare_actor(self, aref: ActorRefT,
                             beacon: NodeT) -> ActorRefT:
        coro: Any
        if isinstance(aref, Awaitable):
            # agent does not yield
            coro = aref
            if self._sinks:
                raise ImproperlyConfigured('Agent must yield to use sinks')
        else:
            # agent yields and is an AsyncIterator so we have to consume it.
            coro = self._slurp(aref, aiter(aref))
        task = asyncio.Task(self._execute_task(coro, aref), loop=self.loop)
        task._beacon = beacon  # type: ignore
        aref.actor_task = task
        self._actors.add(aref)
        return aref

    async def _execute_task(self, coro: Awaitable, aref: ActorRefT) -> None:
        # This executes the agent task itself, and does exception handling.
        try:
            await coro
        except asyncio.CancelledError:
            if self.should_stop:
                raise
        except Exception as exc:
            if self._on_error is not None:
                await self._on_error(self, exc)

            # Mark ActorRef as dead, so that supervisor thread
            # can start a new one.
            assert aref.supervisor is not None
            await aref.crash(exc)
            self._service.supervisor.wakeup()

            raise

    async def _slurp(self, res: ActorRefT, it: AsyncIterator) -> None:
        # this is used when the agent returns an AsyncIterator,
        # and simply consumes that async iterator.
        stream: Optional[StreamT] = None
        async for value in it:
            self.log.debug('%r yielded: %r', self.fun, value)
            if stream is None:
                stream = res.stream.get_active_stream()
            event = stream.current_event
            if event is not None:
                if isinstance(event.value, ReqRepRequest):
                    await self._reply(event.key, value, event.value)
            else:
                raise TypeError('Stream has no current event')
            await self._delegate_to_sinks(value)

    async def _delegate_to_sinks(self, value: Any) -> None:
        for sink in self._sinks:
            if isinstance(sink, AgentT):
                await cast(AgentT, sink).send(value=value)
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

    async def cast(self,
                   value: V = None,
                   *,
                   key: K = None,
                   partition: int = None) -> None:
        await self.send(key, value, partition=partition)

    async def ask(self,
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
            reply_to=reply_to or self.app.conf.reply_to,
            correlation_id=correlation_id,
            force=True,  # Send immediately, since we are waiting for result.
        )
        app = cast(App, self.app)
        await app._reply_consumer.add(p.correlation_id, p)
        await app.maybe_start_client()
        return await p

    async def ask_nowait(self,
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

    def _create_req(self,
                    key: K = None,
                    value: V = None,
                    reply_to: ReplyToArg = None,
                    correlation_id: str = None) -> ReqRepRequest:
        if reply_to is None:
            raise TypeError('Missing reply_to argument')
        topic_name = self._get_strtopic(reply_to)
        correlation_id = correlation_id or str(uuid4())
        return ReqRepRequest(
            value=value,
            reply_to=topic_name,
            correlation_id=correlation_id,
        )

    async def send(self,
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
        """Send message to topic used by agent."""
        if reply_to:
            value = self._create_req(key, value, reply_to, correlation_id)
        return await self.channel.send(
            key,
            value,
            partition,
            key_serializer,
            value_serializer,
            force=force,
        )

    def _get_strtopic(self,
                      topic: Union[str, ChannelT, TopicT, AgentT]) -> str:
        if isinstance(topic, AgentT):
            return self._get_strtopic(cast(AgentT, topic).channel)
        if isinstance(topic, TopicT):
            return cast(TopicT, topic).get_topic_name()
        if isinstance(topic, ChannelT):
            raise ValueError('Channels are unnamed topics')
        return cast(str, topic)

    async def map(self,
                  values: Union[AsyncIterable, Iterable],
                  key: K = None,
                  reply_to: ReplyToArg = None,
                  ) -> AsyncIterator:  # pragma: no cover
        # Map takes only values, but can provide one key that is used for all.
        async for value in self.kvmap(
                ((key, v) async for v in aiter(values)), reply_to):
            yield value

    async def kvmap(
            self,
            items: Union[AsyncIterable[Tuple[K, V]], Iterable[Tuple[K, V]]],
            reply_to: ReplyToArg = None,
    ) -> AsyncIterator[str]:  # pragma: no cover
        # kvmap takes (key, value) pairs.
        reply_to = self._get_strtopic(reply_to or self.app.conf.reply_to)

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

    async def join(self,
                   values: Union[AsyncIterable[V], Iterable[V]],
                   key: K = None,
                   reply_to: ReplyToArg = None,
                   ) -> List[Any]:  # pragma: no cover
        return await self.kvjoin(
            ((key, value) async for value in aiter(values)),
            reply_to=reply_to,
        )

    async def kvjoin(
            self,
            items: Union[AsyncIterable[Tuple[K, V]], Iterable[Tuple[K, V]]],
            reply_to: ReplyToArg = None) -> List[Any]:  # pragma: no cover
        reply_to = self._get_strtopic(reply_to or self.app.conf.reply_to)
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
            self, barrier: BarrierState,
            items: Union[AsyncIterable[Tuple[K, V]], Iterable[Tuple[K, V]]],
            reply_to: ReplyToArg) -> AsyncIterator[str]:  # pragma: no cover
        # map: send many tasks to agents
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
            app = cast(App, self.app)
            await app.maybe_start_client()
            await app._reply_consumer.add(p.correlation_id, barrier)

            yield correlation_id

    def _repr_info(self) -> str:
        return shorten_fqdn(self.name)

    def get_topic_names(self) -> Iterable[str]:
        channel = self.channel
        if isinstance(channel, TopicT):
            return channel.topics
        return []

    @property
    def channel(self) -> ChannelT:
        if self._channel is None:
            self._channel = self._prepare_channel(
                self._channel_arg,
                key_type=self._key_type,
                value_type=self._value_type,
                **self._channel_kwargs,
            )
        return self._channel

    @channel.setter
    def channel(self, channel: ChannelT) -> None:
        self._channel = channel

    @property
    def channel_iterator(self) -> AsyncIterator:
        # The channel is "memoized" here, so subsequent access to
        # instance.channel_iterator will return the same value.
        if self._channel_iterator is None:
            # we do not use aiter(channel) here, because
            # that will also add it to the topic conductor too early.
            self._channel_iterator = self.channel.clone(is_iterator=True)
        return self._channel_iterator

    @channel_iterator.setter
    def channel_iterator(self, it: AsyncIterator) -> None:
        self._channel_iterator = it

    @cached_property
    def _service(self) -> AgentService:
        return AgentService(self, beacon=self.app.beacon, loop=self.app.loop)

    @property
    def label(self) -> str:
        return self._agent_label()

    def _agent_label(self, name_suffix: str = '') -> str:
        s = f'{type(self).__name__}{name_suffix}: '
        s += f'{shorten_fqdn(qualname(self.fun))}'
        return s


class AgentTestWrapper(Agent, AgentTestWrapperT):  # pragma: no cover

    _stream: StreamT

    def __init__(self,
                 *args: Any,
                 original_channel: ChannelT = None,
                 **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.results = {}
        self.new_value_processed = asyncio.Condition(loop=self.loop)
        self.original_channel = cast(ChannelT, original_channel)
        self.add_sink(self._on_value_processed)
        self._stream = self.channel.stream()
        self.sent_offset = 0
        self.processed_offset = 0

    def stream(self, *args: Any, **kwargs: Any) -> StreamT:
        return self._stream.get_active_stream()

    async def on_stop(self) -> None:
        self.results.clear()
        await super().on_stop()

    async def _on_value_processed(self, value: Any) -> None:
        async with self.new_value_processed:
            self.results[self.processed_offset] = value
            self.processed_offset += 1
            self.new_value_processed.notify_all()

    async def put(self,
                  value: V = None,
                  key: K = None,
                  partition: Optional[int] = None,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None,
                  *,
                  reply_to: ReplyToArg = None,
                  correlation_id: str = None,
                  wait: bool = True) -> EventT:
        if reply_to:
            value = self._create_req(key, value, reply_to, correlation_id)
        channel = cast(ChannelT, self.stream().channel)
        message = self.to_message(
            key, value, partition=partition, offset=self.sent_offset)
        event: EventT = await channel.decode(message)
        await channel.put(event)
        self.sent_offset += 1
        if wait:
            async with self.new_value_processed:
                await self.new_value_processed.wait()
        return event

    def to_message(self,
                   key: K,
                   value: V,
                   *,
                   partition: Optional[int] = None,
                   offset: int = 0,
                   timestamp: float = None,
                   timestamp_type: str = 'unix') -> Message:
        try:
            topic_name = self._get_strtopic(self.original_channel)
        except ValueError:
            topic_name = '<internal>'
        return Message(
            topic=topic_name,
            partition=partition or 0,
            offset=offset,
            timestamp=timestamp or time(),
            timestamp_type=timestamp_type,
            key=key,
            value=value,
            checksum=b'',
            serialized_key_size=0,
            serialized_value_size=0,
        )

    async def throw(self, exc: BaseException) -> None:
        await self.stream().throw(exc)

    def __aiter__(self) -> AsyncIterator:
        return aiter(self._stream())
