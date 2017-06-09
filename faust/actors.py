import asyncio
from typing import (
    Any, AsyncIterable, AsyncIterator, Awaitable,
    Iterable, List, MutableSequence, Union, cast,
)
from .types import AppT, CodecArg, K, StreamT, TopicT, V
from .types.actors import ActorFun, ActorErrorHandler, ActorT, SinkT
from .utils.aiter import aiter
from .utils.collections import NodeT
from .utils.futures import maybe_async
from .utils.imports import qualname
from .utils.logging import get_logger
from .utils.objects import cached_property
from .utils.services import Service, ServiceProxy

logger = get_logger(__name__)

__all__ = ['Actor', 'ActorFun', 'ActorT', 'SinkT']

# --- An actor is an `async def` function, iterating over a stream:
#
#   @app.actor(withdrawals_topic)
#   async def alert_on_large_transfer(withdrawal):
#       async for withdrawal in withdrawals:
#           if withdrawal.amount > 1000.0:
#               alert(f'Large withdrawal: {withdrawal}')
#
# unlike normal actors they do not implement replies... yet!


class ActorInstance(Service):
    logger = logger

    agent: ActorT
    stream: StreamT
    actor_task: asyncio.Task = None
    #: If multiple instance are started for concurrency, this is its index.
    index: int = None

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
        return self.shortlabel

    @property
    def shortlabel(self) -> str:
        return f'Actor*: {self.agent.name}'


class AsyncIterableActor(ActorInstance, AsyncIterable):
    it: AsyncIterable

    def __init__(self,
                 agent: ActorT,
                 stream: StreamT,
                 it: AsyncIterable,
                 **kwargs: Any) -> None:
        self.agent = agent
        self.stream = stream
        self.it = it
        super().__init__(**kwargs)

    def __aiter__(self) -> AsyncIterator:
        return self.it.__aiter__()


class AwaitableActor(ActorInstance, Awaitable):
    coro: Awaitable

    def __init__(self,
                 agent: ActorT,
                 stream: StreamT,
                 coro: Awaitable,
                 **kwargs: Any) -> None:
        self.agent = agent
        self.stream = stream
        self.coro = coro
        super().__init__(**kwargs)

    def __await__(self) -> Any:
        return self.coro.__await__()


ActorInstanceT = Union[AwaitableActor, AsyncIterableActor]


class ActorService(Service):
    # Actors are created at module-scope, and the Service class
    # creates the asyncio loop when created, so we separate the
    # actor service in such a way that we can start it lazily.
    # Actor(ServiceProxy) -> ActorService
    logger = logger

    actor: ActorT
    instances: MutableSequence[ActorInstanceT]

    def __init__(self, actor: ActorT, **kwargs: Any) -> None:
        self.actor = actor
        self.instances = []
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        # start the actor processor.
        for index in range(self.actor.concurrency):
            res = await cast(Actor, self.actor)._start_task(index, self.beacon)
            self.instances.append(res)
            await res.start()

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
                 topic: TopicT = None,
                 concurrency: int = 1,
                 sink: Iterable[SinkT] = None,
                 on_error: ActorErrorHandler = None) -> None:
        self.fun: ActorFun = fun
        self.name = name or qualname(self.fun)
        self.app = app
        self.topic = topic
        self.concurrency = concurrency
        self._sinks = list(sink) if sink is not None else []
        self._on_error: ActorErrorHandler = on_error
        ServiceProxy.__init__(self)

    def __call__(self) -> ActorInstanceT:
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
        stream = self.stream()
        res = self.fun(stream)
        if isinstance(res, Awaitable):
            return AwaitableActor(self, stream, res,
                                  loop=self.loop, beacon=self.beacon)
        return AsyncIterableActor(self, stream, res,
                                  loop=self.loop, beacon=self.beacon)

    def add_sink(self, sink: SinkT) -> None:
        self._sinks.append(sink)

    def stream(self, **kwargs: Any) -> StreamT:
        return self.app.stream(self.source, loop=self.loop, **kwargs)

    async def _start_task(self, index: int, beacon: NodeT) -> ActorInstanceT:
        # If the actor is an async function we simply start it,
        # if it returns an AsyncIterable/AsyncGenerator we start a task
        # that will consume it.
        res = self()
        coro = res if isinstance(res, Awaitable) else self._slurp(aiter(res))
        task = asyncio.Task(self._execute_task(coro), loop=self.loop)
        task._beacon = beacon  # type: ignore
        res.actor_task = task
        res.index = index
        return res

    async def _slurp(self, it: AsyncIterator):
        # this is used when the actor returns an AsyncIterator,
        # and simply consumes that async iterator.
        async for value in it:
            self.log.debug('%r yielded: %r', self.fun, value)
            await self._delegate_to_sinks(value)

    async def _delegate_to_sinks(self, value: Any) -> None:
        for sink in self._sinks:
            await maybe_async(sink(value))

    async def _execute_task(self, coro: Awaitable) -> None:
        # This executes the actor task itself, and does exception handling.
        try:
            await coro
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if self._on_error is not None:
                await self._on_error(self, exc)
            raise

    async def send(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            *,
            wait: bool = True) -> Awaitable:
        """Send message to topic used by actor."""
        return await self.topic.send(key, value, partition,
                                     key_serializer, value_serializer,
                                     wait=wait)

    def send_soon(self, key: K, value: V,
                  partition: int = None,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None) -> None:
        """Send message eventually (non async), to topic used by actor."""
        return self.topic.send_soon(key, value, partition,
                                    key_serializer, value_serializer)

    @cached_property
    def source(self) -> AsyncIterator:
        # The source is reused here, so that when ActorService start
        # is called it will start n * concurrency self._start_task() futures
        # that all share the same source topic.
        return aiter(self.topic)

    @cached_property
    def _service(self) -> ActorService:
        return ActorService(self, beacon=self.app.beacon, loop=self.app.loop)

    @property
    def label(self) -> str:
        return f'{type(self).__name__}: {qualname(self.fun)}'

    @property
    def shortlabel(self) -> str:
        return f'{type(self).__name__}{{{qualname(self.fun)}}}'
