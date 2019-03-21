"""Agent implementation."""
import asyncio
import typing
from contextvars import ContextVar
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
from mode.utils.aiter import aenumerate, aiter
from mode.utils.compat import want_bytes, want_str
from mode.utils.futures import maybe_async
from mode.utils.objects import canonshortname, qualname
from mode.utils.text import shorten_fqdn
from mode.utils.types.trees import NodeT

from faust.exceptions import ImproperlyConfigured
from faust.utils.tracing import traced_from_parent_span

from faust.types import (
    AppT,
    ChannelT,
    CodecArg,
    EventT,
    HeadersArg,
    K,
    Message,
    MessageSentCallback,
    ModelArg,
    ModelT,
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
from faust.types.core import merge_headers, prepare_headers

from .actor import Actor, AsyncIterableActor, AwaitableActor
from .models import (
    ModelReqRepRequest,
    ModelReqRepResponse,
    ReqRepRequest,
    ReqRepResponse,
)
from .replies import BarrierState, ReplyPromise

if typing.TYPE_CHECKING:  # pragma: no cover
    from faust.app.base import App as _App
else:
    class _App: ...   # noqa

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
#   app = faust.App('myid', broker='kafka://localhost:9092')
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


_current_agent: ContextVar[Optional[AgentT]]
_current_agent = ContextVar('current_agent')


def current_agent() -> Optional[AgentT]:
    return _current_agent.get(None)


class Agent(AgentT, Service):
    """Agent.

    This is the type of object returned by the ``@app.agent`` decorator.
    """

    supervisor: SupervisorStrategyT = None
    instances: MutableSequence[ActorRefT]

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
                 use_reply_headers: bool = None,
                 **kwargs: Any) -> None:
        self.app = app
        self.fun: AgentFun = fun
        self.name = name or canonshortname(self.fun)
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
        self.use_reply_headers = use_reply_headers
        Service.__init__(self)

    def on_init_dependencies(self) -> Iterable[ServiceT]:
        # Agent service is now a child of app.
        self.beacon.reattach(self.app.beacon)
        return []

    async def _start_one(self,
                         *,
                         index: Optional[int] = None,
                         active_partitions: Optional[Set[TP]] = None,
                         stream: StreamT = None,
                         channel: ChannelT = None) -> ActorT:
        # an index of None means there's only one instance,
        # and `index is None` is used as a test by functions that
        # disallows concurrency.
        index = index if self.concurrency > 1 else None
        return await self._start_task(
            index=index,
            active_partitions=active_partitions,
            stream=stream,
            channel=channel,
            beacon=self.beacon,
        )

    async def _start_one_supervised(
            self,
            index: Optional[int] = None,
            active_partitions: Optional[Set[TP]] = None,
            stream: StreamT = None) -> ActorT:
        aref = await self._start_one(
            index=index,
            active_partitions=active_partitions,
            stream=stream,
        )
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
            index=index,
            active_partitions=aref.active_partitions,
            stream=aref.stream,
            channel=cast(ChannelT, aref.stream.channel),
        )

    def _get_supervisor_strategy(self) -> Type[SupervisorStrategyT]:
        SupervisorStrategy = self.supervisor_strategy
        if SupervisorStrategy is None:
            SupervisorStrategy = self.app.conf.agent_supervisor
        return SupervisorStrategy

    async def _on_start_supervisor(self) -> None:
        active_partitions = self._get_active_partitions()
        channel: ChannelT = cast(ChannelT, None)
        for i in range(self.concurrency):
            res = await self._start_one(
                index=i,
                active_partitions=active_partitions,
                channel=channel,
            )
            if channel is None:
                # First concurrency actor creates channel,
                # then we reuse it for --concurrency=n.
                # This way they share the same queue.
                channel = res.stream.channel
            self.supervisor.add(res)
        await self.supervisor.start()

    def _get_active_partitions(self) -> Optional[Set[TP]]:
        active_partitions: Optional[Set[TP]] = None
        if self.isolated_partitions:
            # when we start our first agent, we create the set of
            # partitions early, and save it in ._pending_active_partitions.
            # That way we can update the set once partitions are assigned,
            # and the actor we started may be assigned one of the partitions.
            active_partitions = self._pending_active_partitions = set()
        return active_partitions

    async def on_stop(self) -> None:
        # Agents iterate over infinite streams, so we cannot wait for it
        # to stop.
        # Instead we cancel it and this forces the stream to ack the
        # last message processed (but not the message causing the error
        # to be raised).
        await self._stop_supervisor()
        await asyncio.gather(*[
            aref.actor_task for aref in self._actors
            if aref.actor_task is not None
        ])
        self._actors.clear()

    async def _stop_supervisor(self) -> None:
        if self.supervisor:
            await self.supervisor.stop()
            self.supervisor = None

    def cancel(self) -> None:
        for aref in self._actors:
            aref.cancel()

    async def on_partitions_revoked(self, revoked: Set[TP]) -> None:
        T = traced_from_parent_span()
        if self.isolated_partitions:
            # isolated: start/stop actors for each partition
            await T(self.on_isolated_partitions_revoked)(revoked)
        else:
            await T(self.on_shared_partitions_revoked)(revoked)

    async def on_partitions_assigned(self, assigned: Set[TP]) -> None:
        T = traced_from_parent_span()
        if self.isolated_partitions:
            await T(self.on_isolated_partitions_assigned)(assigned)
        else:
            await T(self.on_shared_partitions_assigned)(assigned)

    async def on_isolated_partitions_revoked(self, revoked: Set[TP]) -> None:
        self.log.dev('Partitions revoked')
        T = traced_from_parent_span()
        for tp in revoked:
            aref: Optional[ActorRefT] = self._actor_by_partition.pop(tp, None)
            if aref is not None:
                await T(aref.on_isolated_partition_revoked)(tp)

    async def on_isolated_partitions_assigned(self, assigned: Set[TP]) -> None:
        T = traced_from_parent_span()
        for tp in sorted(assigned):
            await T(self._assign_isolated_partition)(tp)

    async def _assign_isolated_partition(self, tp: TP) -> None:
        T = traced_from_parent_span()
        if (not self._first_assignment_done and
                not self._actor_by_partition):
            self._first_assignment_done = True
            # if this is the first time we are assigned
            # we need to reassign the agent we started at boot to
            # one of the partitions.
            T(self._on_first_isolated_partition_assigned)(tp)
        await T(self._maybe_start_isolated)(tp)

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
        return await self._start_for_partitions({tp})

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
                     on_error: AgentErrorHandler = None,
                     **kwargs: Any) -> AgentTestWrapperT:  # pragma: no cover
        # flow control into channel queues are disabled at startup,
        # so need to resume that.
        self.app.flow_control.resume()

        async def on_agent_error(agent: AgentT, exc: BaseException) -> None:
            if on_error is not None:
                await on_error(agent, exc)
            await agent.crash_test_agent(exc)

        return self.clone(
            cls=AgentTestWrapper,
            channel=channel if channel is not None else self.app.channel(),
            supervisor_strategy=supervisor_strategy or CrashingSupervisor,
            original_channel=self.channel,
            on_error=on_agent_error,
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
                 stream: StreamT = None,
                 channel: ChannelT = None) -> ActorRefT:
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
        return self.actor_from_stream(stream,
                                      index=index,
                                      active_partitions=active_partitions,
                                      channel=channel)

    def actor_from_stream(self,
                          stream: Optional[StreamT],
                          *,
                          index: int = None,
                          active_partitions: Set[TP] = None,
                          channel: ChannelT = None) -> ActorRefT:
        we_created_stream = False
        actual_stream: StreamT
        if stream is None:
            actual_stream = self.stream(
                channel=channel,
                concurrency_index=index,
                active_partitions=active_partitions,
            )
            we_created_stream = True
        else:
            # reusing actor stream after agent restart
            assert stream.concurrency_index == index
            assert stream.active_partitions == active_partitions
            actual_stream = stream

        res = self.fun(actual_stream)
        if isinstance(res, AsyncIterable):
            if we_created_stream:
                actual_stream.add_processor(self._maybe_unwrap_reply_request)
            typ = cast(Type[Actor], AsyncIterableActor)
        else:
            typ = cast(Type[Actor], AwaitableActor)
        return typ(
            self,
            actual_stream,
            res,
            index=actual_stream.concurrency_index,
            active_partitions=actual_stream.active_partitions,
            loop=self.loop,
            beacon=self.beacon,
        )

    def add_sink(self, sink: SinkT) -> None:
        if sink not in self._sinks:
            self._sinks.append(sink)

    def stream(self,
               channel: ChannelT = None,
               active_partitions: Set[TP] = None,
               **kwargs: Any) -> StreamT:
        if channel is None:
            channel = cast(TopicT, self.channel_iterator).clone(
                is_iterator=False,
                active_partitions=active_partitions,
            )
        if active_partitions is not None:
            assert channel.active_partitions == active_partitions
        s = self.app.stream(
            channel,
            loop=self.loop,
            active_partitions=active_partitions,
            prefix=self.name,
            **kwargs)
        return s

    def _maybe_unwrap_reply_request(self, value: V) -> Any:
        if isinstance(value, ReqRepRequest):
            return value.value
        return value

    async def _start_task(self,
                          *,
                          index: Optional[int],
                          active_partitions: Optional[Set[TP]] = None,
                          stream: StreamT = None,
                          channel: ChannelT = None,
                          beacon: NodeT = None) -> ActorRefT:
        # If the agent is an async function we simply start it,
        # if it returns an AsyncIterable/AsyncGenerator we start a task
        # that will consume it.
        actor = self(
            index=index,
            active_partitions=active_partitions,
            stream=stream,
            channel=channel,
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
        _current_agent.set(self)
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
            await aref.crash(exc)
            self.supervisor.wakeup()

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
                headers = event.headers
                reply_to: Optional[str] = None
                correlation_id: Optional[str] = None
                if isinstance(event.value, ReqRepRequest):
                    req: ReqRepRequest = event.value
                    reply_to = req.reply_to
                    correlation_id = req.correlation_id
                elif headers:
                    reply_to = want_str(headers.get('Faust-Ag-ReplyTo'))
                    correlation_id = want_str(headers.get(
                        'Faust-Ag-CorrelationId'))
                if reply_to is not None:
                    await self._reply(
                        event.key, value, reply_to, cast(str, correlation_id))
            await self._delegate_to_sinks(value)

    async def _delegate_to_sinks(self, value: Any) -> None:
        for sink in self._sinks:
            if isinstance(sink, AgentT):
                await cast(AgentT, sink).send(value=value)
            elif isinstance(sink, ChannelT):
                await cast(TopicT, sink).send(value=value)
            else:
                await maybe_async(cast(Callable, sink)(value))

    async def _reply(self, key: Any, value: Any,
                     reply_to: str, correlation_id: str) -> None:
        assert reply_to
        response = self._response_class(value)(
            key=key,
            value=value,
            correlation_id=correlation_id,
        )
        await self.app.send(
            reply_to,
            key=None,
            value=response,
        )

    def _response_class(self, value: Any) -> Type[ReqRepResponse]:
        if isinstance(value, ModelT):
            return ModelReqRepResponse
        return ReqRepResponse

    async def cast(self,
                   value: V = None,
                   *,
                   key: K = None,
                   partition: int = None,
                   timestamp: float = None,
                   headers: HeadersArg = None) -> None:
        await self.send(
            key=key,
            value=value,
            partition=partition,
            timestamp=timestamp,
            headers=headers,
        )

    async def ask(self,
                  value: V = None,
                  *,
                  key: K = None,
                  partition: int = None,
                  timestamp: float = None,
                  headers: HeadersArg = None,
                  reply_to: ReplyToArg = None,
                  correlation_id: str = None) -> Any:
        p = await self.ask_nowait(
            value,
            key=key,
            partition=partition,
            timestamp=timestamp,
            headers=headers,
            reply_to=reply_to or self.app.conf.reply_to,
            correlation_id=correlation_id,
            force=True,  # Send immediately, since we are waiting for result.
        )
        app = cast(_App, self.app)
        await app._reply_consumer.add(p.correlation_id, p)
        await app.maybe_start_client()
        return await p

    async def ask_nowait(self,
                         value: V = None,
                         *,
                         key: K = None,
                         partition: int = None,
                         timestamp: float = None,
                         headers: HeadersArg = None,
                         reply_to: ReplyToArg = None,
                         correlation_id: str = None,
                         force: bool = False) -> ReplyPromise:
        if reply_to is None:
            raise TypeError('Missing reply_to argument')
        reply_to = self._get_strtopic(reply_to)
        correlation_id = correlation_id or str(uuid4())
        value, headers = self._create_req(
            key, value, reply_to, correlation_id, headers)
        await self.channel.send(
            key=key,
            value=value,
            partition=partition,
            timestamp=timestamp,
            headers=headers,
            force=force,
        )
        return ReplyPromise(reply_to, correlation_id)

    def _create_req(
            self,
            key: K = None,
            value: V = None,
            reply_to: ReplyToArg = None,
            correlation_id: str = None,
            headers: HeadersArg = None) -> Tuple[V, Optional[HeadersArg]]:
        if reply_to is None:
            raise TypeError('Missing reply_to argument')
        topic_name = self._get_strtopic(reply_to)
        correlation_id = correlation_id or str(uuid4())
        open_headers = prepare_headers(headers or {})
        if self.use_reply_headers:
            merge_headers(open_headers, {
                'Faust-Ag-ReplyTo': want_bytes(topic_name),
                'Faust-Ag-CorrelationId': want_bytes(correlation_id),
            })
            return value, open_headers
        else:
            # wrap value in envelope
            req = self._request_class(value)(
                value=value,
                reply_to=topic_name,
                correlation_id=correlation_id,
            )
            return req, open_headers

    def _request_class(self, value: V) -> Type[ReqRepRequest]:
        if isinstance(value, ModelT):
            return ModelReqRepRequest
        return ReqRepRequest

    async def send(self,
                   *,
                   key: K = None,
                   value: V = None,
                   partition: int = None,
                   timestamp: float = None,
                   headers: HeadersArg = None,
                   key_serializer: CodecArg = None,
                   value_serializer: CodecArg = None,
                   callback: MessageSentCallback = None,
                   reply_to: ReplyToArg = None,
                   correlation_id: str = None,
                   force: bool = False) -> Awaitable[RecordMetadata]:
        """Send message to topic used by agent."""
        if reply_to:
            value, headers = self._create_req(
                key, value, reply_to, correlation_id, headers)
        return await self.channel.send(
            key=key,
            value=value,
            partition=partition,
            timestamp=timestamp,
            headers=headers,
            key_serializer=key_serializer,
            value_serializer=value_serializer,
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
            app = cast(_App, self.app)
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
            self._channel_iterator = self.channel.clone(is_iterator=False)
        return self._channel_iterator

    @channel_iterator.setter
    def channel_iterator(self, it: AsyncIterator) -> None:
        self._channel_iterator = it

    @property
    def label(self) -> str:
        return self._agent_label()

    def _agent_label(self, name_suffix: str = '') -> str:
        s = f'{type(self).__name__}{name_suffix}: '
        s += f'{shorten_fqdn(qualname(self.fun))}'
        return s

    @property
    def shortlabel(self) -> str:
        return self._agent_label()


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

    async def _on_value_processed(self, value: Any) -> None:
        async with self.new_value_processed:
            self.results[self.processed_offset] = value
            self.processed_offset += 1
            self.new_value_processed.notify_all()

    async def crash_test_agent(self, exc: BaseException) -> None:
        self._crash(exc)
        async with self.new_value_processed:
            self.new_value_processed.notify_all()

    async def put(self,
                  value: V = None,
                  key: K = None,
                  partition: Optional[int] = None,
                  timestamp: Optional[float] = None,
                  headers: HeadersArg = None,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None,
                  *,
                  reply_to: ReplyToArg = None,
                  correlation_id: str = None,
                  wait: bool = True) -> EventT:
        if reply_to:
            value, headers = self._create_req(
                key, value, reply_to, correlation_id, headers)
        channel = cast(ChannelT, self.stream().channel)
        message = self.to_message(
            key, value,
            partition=partition,
            offset=self.sent_offset,
            timestamp=timestamp,
            headers=headers,
        )
        event: EventT = await channel.decode(message)
        await channel.put(event)
        self.sent_offset += 1
        if wait:
            async with self.new_value_processed:
                await self.new_value_processed.wait()
                if self._crash_reason:
                    raise self._crash_reason from self._crash_reason
        return event

    def to_message(self,
                   key: K,
                   value: V,
                   *,
                   partition: Optional[int] = None,
                   offset: int = 0,
                   timestamp: float = None,
                   timestamp_type: int = 0,
                   headers: HeadersArg = None) -> Message:
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
            headers=headers,
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
