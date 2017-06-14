import abc
import typing
from typing import (
    Any, AsyncIterable, AsyncIterator, Awaitable, Callable, Iterable, Union,
)
from ..utils.types.services import ServiceT
from .codecs import CodecArg
from .core import K, V
from .streams import StreamT
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

#: A sink can be: Actor, Topic,
#: or callable/async callable taking value as argument.
SinkT = Union['ActorT', TopicT, Callable[[Any], Union[Awaitable, None]]]


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
                 sink: Iterable[SinkT] = None,
                 on_error: ActorErrorHandler = None) -> None:
        self.fun: ActorFun = fun

    @abc.abstractmethod
    def __call__(self) -> Union[Awaitable, AsyncIterable]:
        ...

    @abc.abstractmethod
    def add_sink(self, sink: SinkT) -> None:
        ...

    @abc.abstractmethod
    def stream(self, **kwargs: Any) -> StreamT:
        ...

    @abc.abstractmethod
    async def cast(
            self,
            key: K = None,
            value: V = None,
            partition: int = None) -> None:
        ...

    @abc.abstractmethod
    async def ask(
            self,
            key: K = None,
            value: V = None,
            partition: int = None,
            reply_to: Union[str, TopicT] = None,
            correlation_id: str = None) -> Any:
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
            reply_to: Union[str, TopicT, 'ActorT'] = None,
            correlation_id: str = None,
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
