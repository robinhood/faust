import abc
import typing
from typing import AsyncIterable, AsyncIterator, Awaitable, Callable, Union
from ..utils.types.services import ServiceT
from .codecs import CodecArg
from .core import K, V
from .topics import TopicT

if typing.TYPE_CHECKING:
    from .app import AppT
else:
    class AppT: ...  # noqa

__all__ = [
    'ActorErrorHandler',
    'ActorFun',
    'ActorT',
]

ActorErrorHandler = Callable[['ActorT', Exception], Awaitable]
ActorFun = Callable[[AsyncIterator], Union[Awaitable, AsyncIterable]]


class ActorT(ServiceT):

    name: str
    app: AppT
    topic: TopicT
    concurrency: int

    @abc.abstractmethod
    def __init__(self, fun: ActorFun,
                 *,
                 name: str = None,
                 app: AppT = None,
                 topic: TopicT = None,
                 concurrency: int = 1,
                 on_error: ActorErrorHandler = None) -> None:
        self.fun: ActorFun = fun

    @abc.abstractmethod
    def __call__(self) -> Union[Awaitable, AsyncIterable]:
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
