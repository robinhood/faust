from typing import AsyncIterable, AsyncIterator, Awaitable
from .types import AppT, CodecArg, K, TopicT, V
from .types.actors import ActorFun, ActorT
from .utils.aiter import aiter
from .utils.logging import get_logger
from .utils.objects import cached_property
from .utils.services import Service, ServiceProxy

logger = get_logger(__name__)

__all__ = ['Actor', 'ActorFun', 'ActorT']


class ActorService(Service):

    def __init__(self, actor: 'ActorT') -> None:
        self.actor = actor

    async def on_start(self) -> None:
        ...


class Actor(ActorT, ServiceProxy):

    app: AppT
    topic: TopicT
    concurrency: int

    def __init__(self, app: AppT, topic: TopicT, fun: ActorFun,
                 *,
                 concurrency: int = 1) -> None:
        self.app = app
        self.topic = topic
        self.fun: ActorFun = fun
        self.concurrency = concurrency

    def __call__(self, app: AppT) -> Awaitable:
        result = self.fun(self.app.stream(self.source))
        if isinstance(result, AsyncIterable):
            # if actor returns async generator we must consume
            # what it produces.
            return self._slurp(aiter(result))
        return result

    async def _slurp(self, it: AsyncIterator):
        async for item in it:
            logger.debug('Actor %r yielded: %r', self.fun, item)

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
        return ActorService(self)
