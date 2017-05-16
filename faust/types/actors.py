import abc
import typing
from typing import AsyncIterable, AsyncIterator, Awaitable, Callable, Union
from .core import K, V
from .serialization import CodecArg
from .topics import TopicT

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...  # noqa

ActorFun = Callable[[AsyncIterator], Union[Awaitable, AsyncIterable]]


class ActorT(abc.ABC):

    app: AppT
    topic: TopicT
    concurrency: int

    @abc.abstractmethod
    def __init__(self, app: AppT, topic: TopicT, fun: ActorFun,
                 *,
                 concurrency: int = 1) -> None:
        self.fun: ActorFun = fun

    @abc.abstractmethod
    def __call__(self, app: AppT) -> Awaitable:
        ...

    @abc.abstractmethod
    async def send(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            key_serializer: CodecArg = None,
            value_serializer: CodecArg = None,
            *,
            wait: bool = True) -> Awaitable:
        ...

    @abc.abstractmethod
    def send_soon(self, key: K, value: V,
                  partition: int = None,
                  key_serializer: CodecArg = None,
                  value_serializer: CodecArg = None) -> None:
        ...

    @property
    @abc.abstractmethod
    def source(self) -> AsyncIterator:
        ...

    @source.setter
    def source(self, source: AsyncIterator) -> None:
        ...
