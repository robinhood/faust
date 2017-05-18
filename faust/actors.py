import asyncio
from typing import Any, AsyncIterable, AsyncIterator, Awaitable, Union, cast
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


class ActorService(Service):
    actor: ActorT
    task: asyncio.Task

    def __init__(self, actor: 'ActorT', **kwargs: Any) -> None:
        self.actor = actor
        super().__init__(**kwargs)

    async def on_start(self) -> None:
        self.task = await cast(Actor, self.actor)._start_task(self.beacon)
        self.add_future(self.task)

    async def on_stop(self) -> None:
        self.task.cancel()


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
        return self.fun(self.app.stream(self.source))

    async def _slurp(self, it: AsyncIterator):
        async for item in it:
            logger.debug('Actor %r yielded: %r', self.fun, item)

    async def _start_task(self, beacon: NodeT) -> asyncio.Task:
        res = self()
        coro = res if isinstance(res, Awaitable) else self._slurp(aiter(res))
        task = asyncio.Task(self._execute_task(coro), loop=self.loop)
        task._beacon = beacon  # type: ignore
        return task

    async def _execute_task(self, coro: Awaitable) -> None:
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
        return await self.topic.send(key, value, partition,
                                     key_serializer, value_serializer,
                                     wait=wait)

    def send_soon(self, key: K, value: V,
                  partition: int = None,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None) -> None:
        return self.topic.send_soon(key, value, partition,
                                    key_serializer, value_serializer)

    @cached_property
    def source(self) -> AsyncIterator:
        return aiter(self.topic)

    @cached_property
    def _service(self) -> ActorService:
        return ActorService(self, beacon=self.app.beacon, loop=self.app.loop)
