"""Agent implementation."""
import asyncio
import typing

from typing import (
    Any, AsyncIterable, AsyncIterator, Awaitable, Callable,
    Iterable, List, MutableMapping, MutableSequence, Tuple, Type, Union, cast,
)
from uuid import uuid4

from mode import OneForOneSupervisor, Service, ServiceT, SupervisorStrategyT
from mode.proxy import ServiceProxy
from mode.utils.types.trees import NodeT

from .models import ReqRepRequest, ReqRepResponse
from .replies import BarrierState, ReplyPromise

from ..types import (
    AppT, ChannelT, CodecArg, K, MessageSentCallback, ModelArg,
    RecordMetadata, StreamT, TopicT, V,
)
from ..types.agents import (
    ActorRefT, ActorT, AgentErrorHandler, AgentFun, AgentT,
    AsyncIterableActorT, AwaitableActorT, ReplyToArg, SinkT, _T,
)
from ..utils.aiter import aenumerate, aiter
from ..utils.futures import maybe_async
from ..utils.objects import cached_property, canoname, qualname
from ..utils.text import shorten_fqdn

if typing.TYPE_CHECKING:
    from .app import App
else:
    class App: ...   # noqa

__all__ = [
    'Actor',
    'AsyncIterableActor',
    'AwaitableActor',
    'Agent',
]

SUPERVISOR_STRATEGY: Type[SupervisorStrategyT] = OneForOneSupervisor

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


class Actor(ActorT, Service):
    """An actor is a specific agent instance."""

    # Agent will start n * concurrency actors.

    def __init__(self,
                 agent: AgentT,
                 stream: StreamT,
                 it: _T,
                 index: int = None,
                 **kwargs: Any) -> None:
        self.agent = agent
        self.stream = stream
        self.it = it
        self.index = index
        self.actor_task = None
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
        return f'Agent*: {shorten_fqdn(self.agent.name)}'


class AsyncIterableActor(AsyncIterableActorT, Actor):
    """Used for agent function that yields."""

    def __aiter__(self) -> AsyncIterator:
        return self.it.__aiter__()


class AwaitableActor(AwaitableActorT, Actor):
    """Used for actor function that do not yield."""

    def __await__(self) -> Any:
        return self.it.__await__()


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

    async def _start_one(self, index: int = None) -> ActorT:
        # an index of None means there's only one instance,
        # and `index is None` is used as a test by functions that
        # disallows concurrency.
        index = index if (self.agent.concurrency or 1) > 1 else None
        return await cast(Agent, self.agent)._start_task(index, self.beacon)

    async def on_start(self) -> None:
        self.supervisor = self.agent.supervisor_strategy(
            max_restarts=100.0, over=1.0,
            replacement=self._replace_actor,
            loop=self.loop, beacon=self.beacon,
        )
        for i in range(self.agent.concurrency):
            res = await self._start_one(i)
            self.supervisor.add(res)
        await self.supervisor.start()

    async def _replace_actor(self, _service: ServiceT, index: int) -> ServiceT:
        return await self._start_one(index)

    async def on_stop(self) -> None:
        # Agents iterate over infinite streams, so we cannot wait for it
        # to stop.
        # Instead we cancel it and this forces the stream to ack the
        # last message processed (but not the message causing the error
        # to be raised).
        if self.supervisor:
            await self.supervisor.stop()

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

    _channel_iterator: AsyncIterator = None
    _sinks: List[SinkT] = None

    def __init__(self, fun: AgentFun,
                 *,
                 name: str = None,
                 app: AppT = None,
                 channel: Union[str, ChannelT] = None,
                 concurrency: int = 1,
                 sink: Iterable[SinkT] = None,
                 on_error: AgentErrorHandler = None,
                 supervisor_strategy: Type[SupervisorStrategyT] = None,
                 help: str = None,
                 key_type: ModelArg = None,
                 value_type: ModelArg = None,
                 **kwargs: Any) -> None:
        self.app = app
        self.fun: AgentFun = fun
        self.name = name or canoname(self.fun)
        # key-type/value_type arguments only apply when a channel
        # is not set
        if key_type is not None:
            assert channel is None or isinstance(channel, str)
        if value_type is not None:
            assert channel is None or isinstance(channel, str)
        self.channel = self._prepare_channel(
            channel,
            key_type=key_type,
            value_type=value_type,
            **kwargs)
        self.concurrency = concurrency
        self.help = help
        self._sinks = list(sink) if sink is not None else []
        self._on_error: AgentErrorHandler = on_error
        self.supervisor_strategy = supervisor_strategy or SUPERVISOR_STRATEGY
        ServiceProxy.__init__(self)

    def _prepare_channel(self,
                         channel: Union[str, ChannelT] = None,
                         internal: bool = True,
                         key_type: ModelArg = None,
                         value_type: ModelArg = None,
                         **kwargs: Any) -> ChannelT:
        channel = f'{self.app.id}-{self.name}' if channel is None else channel
        if isinstance(channel, ChannelT):
            return cast(ChannelT, channel)
        elif isinstance(channel, str):
            return self.app.topic(
                channel,
                internal=internal,
                key_type=key_type,
                value_type=value_type,
                **kwargs)
        raise TypeError(
            f'Channel must be channel, topic, or str; not {type(channel)}')

    def __call__(self, *, index: int = None) -> ActorRefT:
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
        stream = self.stream(concurrency_index=index)
        res = self.fun(stream)
        typ = cast(Type[Actor], (
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
        # If the agent is an async function we simply start it,
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
        # This executes the agent task itself, and does exception handling.
        try:
            await coro
        except asyncio.CancelledError:
            if aref.stream.current_event:
                await aref.stream.current_event.ack()
            raise
        except Exception as exc:
            self.log.exception('Agent %r raised error: %r', aref, exc)
            if aref.stream.current_event:
                await aref.stream.current_event.ack()
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
        stream: StreamT = None
        async for value in it:
            self.log.debug('%r yielded: %r', self.fun, value)
            if stream is None:
                stream = res.stream.get_active_stream()
            event = stream.current_event
            assert stream.current_event is not None
            if isinstance(event.value, ReqRepRequest):
                await self._reply(event.key, value, event.value)
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
        app = cast(App, self.app)
        await app._reply_consumer.add(p.correlation_id, p)
        await app.maybe_start_client()
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
        """Send message to topic used by agent."""
        if reply_to:
            value = self._create_req(key, value, reply_to, correlation_id)
        return await self.channel.send(
            key, value, partition,
            key_serializer, value_serializer,
            force=force,
        )

    def _get_strtopic(
            self, topic: Union[str, ChannelT, TopicT, AgentT]) -> str:
        if isinstance(topic, AgentT):
            return self._get_strtopic(cast(AgentT, topic).channel)
        if isinstance(topic, TopicT):
            return cast(TopicT, topic).get_topic_name()
        if isinstance(topic, ChannelT):
            raise ValueError('Channels are unnamed topics')
        return cast(str, topic)

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

    @property
    def channel_iterator(self) -> AsyncIterator:
        # The channel is "memoized" here, so subsequent access to
        # instance.channel_iterator will return the same value.
        if self._channel_iterator is None:
            self._channel_iterator = aiter(self.channel)
        return self._channel_iterator

    @channel_iterator.setter
    def channel_iterator(self, it: AsyncIterator) -> None:
        self._channel_iterator = it

    @cached_property
    def _service(self) -> AgentService:
        return AgentService(self, beacon=self.app.beacon, loop=self.app.loop)

    @property
    def label(self) -> str:
        return f'{type(self).__name__}: {shorten_fqdn(qualname(self.fun))}'
