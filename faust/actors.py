import asyncio
from typing import (
    Any, AsyncIterable, AsyncIterator, Awaitable,
    MutableSequence, Union, cast,
)
from .types import AppT, CodecArg, K, TopicT, V
from .types.actors import ActorFun, ActorErrorHandler, ActorT
from .utils.aiter import aiter
from .utils.collections import NodeT
from .utils.imports import qualname
from .utils.logging import get_logger
from .utils.objects import cached_property
from .utils.services import Service, ServiceProxy

logger = get_logger(__name__)

__all__ = ['Actor', 'ActorFun', 'ActorT']


# --- Actors are asyncio.Tasks that processes a Stream.
# like normal actors they have an inbox and communicate like message passing,
# but they do not have replies yet.


class ActorService(Service):
    # Since actors are created at module-scope, we create the actor
    # service lazily: Actor(ServiceProxy) -> ActorService

    actor: ActorT
    instances: MutableSequence[asyncio.Task]

    def __init__(self, actor: 'ActorT', **kwargs: Any) -> None:
        self.actor = actor
        self.instances = []
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        # start the actor processor.
        for _ in range(self.actor.concurrency):
            task = await cast(Actor, self.actor)._start_task(self.beacon)
            self.add_future(task)
            self.instances.append(task)

    async def on_stop(self) -> None:
        # actors processes infinite streams, so we cannot wait for it to stop.
        # simply cancel it and the stream will ack the last message processed.
        for instance in self.instances:
            instance.cancel()


class Actor(ActorT, ServiceProxy):

    def __init__(self, fun: ActorFun,
                 *,
                 name: str = None,
                 app: AppT = None,
                 topic: TopicT = None,
                 concurrency: int = 1,
                 on_error: ActorErrorHandler = None) -> None:
        self.fun: ActorFun = fun
        self.name = name or qualname(self.fun)
        self.app = app
        self.topic = topic
        self.concurrency = concurrency
        self._on_error: ActorErrorHandler = on_error

    def __call__(self) -> Union[Awaitable, AsyncIterable]:
        # The actor function can be reused by other actors/tasks.
        return self.fun(self.app.stream(self.source))

    async def _start_task(self, beacon: NodeT) -> asyncio.Task:
        # If the actor is an async function we simply start it,
        # if it returns an AsyncIterable/AsyncGenerator we start a task
        # that will consume it.
        res = self()
        coro = res if isinstance(res, Awaitable) else self._slurp(aiter(res))
        task = asyncio.Task(self._execute_task(coro), loop=self.loop)
        task._beacon = beacon  # type: ignore
        return task

    async def _slurp(self, it: AsyncIterator):
        # this is used when the actor returns an AsyncIterable,
        # and simply consumes that async iterator.
        async for item in it:
            logger.debug('Actor %r yielded: %r', self.fun, item)

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
        # The source is shared by multiple instances of this actor.
        return aiter(self.topic)

    @cached_property
    def _service(self) -> ActorService:
        return ActorService(self, beacon=self.app.beacon, loop=self.app.loop)
